"""Per-connection send buffer and retransmission bookkeeping."""

import time


class SendBufferManager:
    """Manage queued and unacknowledged TCP data for one connection.

    Sequence numbers, rwnd, cwnd, and InFlight are measured in bytes.
    N limits the number of segments stored in send_buffer.

    ACK processing uses cumulative ACKs. SACK is not implemented.
    """

    def __init__(
        self,
        initial_seq=0,
        mss=1460,
        max_segments=32,
        rwnd=32 * 1460,
        rto=1.0,
        duplicate_ack_threshold=3,
    ):
        if initial_seq < 0:
            raise ValueError("initial_seq must be non-negative")
        if mss <= 0:
            raise ValueError("mss must be positive")
        if max_segments <= 0:
            raise ValueError("max_segments must be positive")
        if rwnd < 0:
            raise ValueError("rwnd must be non-negative")
        if rto <= 0:
            raise ValueError("rto must be positive")
        if duplicate_ack_threshold <= 0:
            raise ValueError("duplicate_ack_threshold must be positive")

        self.snd_base = initial_seq
        self.next_seq = initial_seq
        self.highest_sent = initial_seq

        self.MSS = mss
        self.N = max_segments
        self.rwnd = rwnd
        self.rto = rto
        self.duplicate_ack_threshold = duplicate_ack_threshold

        # Key is the first byte sequence number of each segment.
        self.send_buffer = {}
        self.timers = {}

        # Sequence numbers queued but not yet transmitted.
        self._pending = []

        # Bytes sent but not yet cumulatively acknowledged.
        self.InFlight = 0

        # Duplicate ACK count per missing sequence number.
        self.out_of_order_tracker = {}

    def push_data(self, payload):
        """Queue up to N segments and return the number of bytes accepted.

        If the buffer has insufficient room, the caller can enqueue the
        remaining bytes later.
        """
        if not isinstance(payload, bytes):
            raise TypeError("payload must be bytes")

        free_slots = self.N - len(self.send_buffer)
        if not payload or free_slots <= 0:
            return 0

        accepted = 0
        offset = 0

        while offset < len(payload) and free_slots > 0:
            chunk = payload[offset:offset + self.MSS]
            seq = self.next_seq

            self.send_buffer[seq] = {
                "payload": chunk,
                "is_acked": False,
                "sent_time": None,
                "retries": 0,
            }
            self._pending.append(seq)

            self.next_seq += len(chunk)
            accepted += len(chunk)
            offset += len(chunk)
            free_slots -= 1

        return accepted

    def can_send(self, cwnd):
        """Return whether the next queued segment fits rwnd and cwnd."""
        if cwnd < 0:
            raise ValueError("cwnd must be non-negative")
        if not self._pending:
            return False

        seq = self._pending[0]
        size = len(self.send_buffer[seq]["payload"])

        return (
            self.InFlight + size <= self.rwnd
            and self.InFlight + size <= cwnd
        )

    def get_next_packet_to_send(self, now=None): #Note
        """Return the next queued segment without marking it as sent.

        Call can_send(cwnd) first.
        Returns (seq, payload), or None if no segment is pending.
        """
        if not self._pending:
            return None

        seq = self._pending[0]
        entry = self.send_buffer[seq]

        return seq, entry["payload"]

    def mark_packet_sent(self, seq, now=None): #Note
        """Mark a queued segment as sent after sendto() succeeds."""
        if not self._pending or self._pending[0] != seq:
            return False

        entry = self.send_buffer.get(seq)
        if entry is None:
            return False

        current_time = time.monotonic() if now is None else now

        # Remove the segment from the pending queue.
        self._pending.pop(0)

        # Record the successful send attempt.
        entry["sent_time"] = current_time
        entry["retries"] += 1
        self.timers[seq] = current_time + self.rto

        # Update the number of unacknowledged bytes.
        self.InFlight += len(entry["payload"])

        # Track the highest sequence number sent.
        self.highest_sent = max(
            self.highest_sent,
            seq + len(entry["payload"]),
        )

        return True

    def on_ack_received(self, ack_seq, rcv_rwnd):
        """Process a cumulative ACK.

        Returns (is_new_ack, loss_seq). loss_seq is set when the same ACK has
        been received duplicate_ack_threshold times, indicating a likely hole.
        """
        if rcv_rwnd < 0:
            raise ValueError("rcv_rwnd must be non-negative")

        # Ignore stale ACKs and ACKs for data that has not been sent.
        if ack_seq < self.snd_base or ack_seq > self.highest_sent:
            return False, None

        window_changed = (rcv_rwnd != self.rwnd) #Note

        self.rwnd = rcv_rwnd #Note

        # Duplicate ACK: the receiver still needs the byte at snd_base.
        if ack_seq == self.snd_base:
            if window_changed: #Note
                self.out_of_order_tracker.pop(ack_seq, None) #Note
                return False, None #Note
            later_segment_in_flight = any(
                seq > ack_seq and entry["sent_time"] is not None
                for seq, entry in self.send_buffer.items()
            )
            if not later_segment_in_flight:
                return False, None

            count = self.out_of_order_tracker.get(ack_seq, 0) + 1
            self.out_of_order_tracker[ack_seq] = count

            if count == self.duplicate_ack_threshold:
                return False, ack_seq
            return False, None

        acknowledged_bytes = ack_seq - self.snd_base
        new_buffer = {}
        new_timers = {}
        new_pending = []

        for seq, entry in sorted(self.send_buffer.items()):
            payload = entry["payload"]
            end_seq = seq + len(payload)

            # The cumulative ACK covers this entire segment.
            if end_seq <= ack_seq:
                continue

            # ACK ends partway through this segment; retain its unacked suffix.
            if seq < ack_seq < end_seq:
                trim = ack_seq - seq
                remaining = payload[trim:]
                new_seq = ack_seq

                updated = dict(entry)
                updated["payload"] = remaining
                updated["is_acked"] = False
                new_buffer[new_seq] = updated

                if seq in self.timers:
                    new_timers[new_seq] = self.timers[seq]
                if seq in self._pending:
                    new_pending.append(new_seq)
                continue

            new_buffer[seq] = entry
            if seq in self.timers:
                new_timers[seq] = self.timers[seq]
            if seq in self._pending:
                new_pending.append(seq)

        self.send_buffer = new_buffer
        self.timers = new_timers
        self._pending = new_pending
        self.InFlight = max(0, self.InFlight - acknowledged_bytes)
        self.snd_base = ack_seq

        # Remove duplicate-ACK counts for holes that are now behind snd_base.
        self.out_of_order_tracker = {
            seq: count
            for seq, count in self.out_of_order_tracker.items()
            if seq >= self.snd_base
        }

        return True, None

    def get_retransmission(self, seq, now=None): #Note
        """Return an unacknowledged segment for retransmission.

        Do not update its timer or retry count yet.
        """
        entry = self.send_buffer.get(seq)

        if entry is None:
            return None

        # Only retransmit segments that have already been sent.
        if entry["sent_time"] is None:
            return None

        return seq, entry["payload"]

    def mark_retransmission_sent(self, seq, now=None): #Note
        """Update retransmission state after sendto() succeeds."""
        entry = self.send_buffer.get(seq)

        if entry is None:
            return False

        # The segment must have been sent at least once.
        if entry["sent_time"] is None:
            return False

        current_time = time.monotonic() if now is None else now

        # Record the successful retransmission attempt.
        entry["sent_time"] = current_time
        entry["retries"] += 1

        # Restart the retransmission timer.
        self.timers[seq] = current_time + self.rto

        return True

    def check_timeouts(self, now=None):
        """Return sequence numbers whose retransmission timers have expired."""
        current_time = time.monotonic() if now is None else now
        return [
            seq
            for seq, deadline in self.timers.items()
            if deadline <= current_time
        ]

    def get_timed_out_packets(self, now=None): #Note
        """Return timed-out packets without updating their send state."""
        current_time = time.monotonic() if now is None else now

        timed_out = self.check_timeouts(current_time)
        packets = []

        for seq in timed_out:
            packet = self.get_retransmission(seq)

            if packet is not None:
                packets.append(packet)

        return packets