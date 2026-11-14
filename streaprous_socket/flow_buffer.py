# -*- coding: utf-8 -*-
"""
StreapRous Flow & Reliability Buffer Module
Tệp tin: streaprous_socket/flow_buffer.py
Phụ trách: Nhóm Flow Control & Nhóm Reliability
Đặc tả kỹ thuật: RFC 5681, RFC 8312 & Selective Repeat ARQ / Dynamic Receiver Window (rwnd)

Quản lý bộ đệm gửi/nhận, cửa sổ trượt và cơ chế điều khiển luồng chống tràn bộ đệm nhận.
Hỗ trợ cả Byte Sequence Number (chuẩn TCP) lẫn đơn vị packet tương thích CUBIC.
"""

import time
from typing import Tuple, List, Optional, Dict, Any

try:
    from . import constants
except ImportError:
    import constants


class SendBufferManager:
    """
    Manage queued and unacknowledged TCP data for one connection.
    
    Tích hợp quản lý phân đoạn theo Byte Sequence Number chuẩn TCP (RFC 5681/8312),
    hàng đợi _pending cho các gói chưa phát, và cơ chế phát hiện mất gói qua Duplicate ACKs.
    """

    def __init__(
        self,
        initial_seq: int = 0,
        mss: int = constants.MSS,
        win_size: Optional[int] = None,
        max_segments: int = constants.SND_WIN_SIZE,
        rwnd: Optional[int] = None,
        rto: float = constants.INITIAL_RTO,
        duplicate_ack_threshold: int = constants.DUP_ACK_THRESHOLD,
    ):
        if initial_seq < 0:
            raise ValueError("initial_seq must be non-negative")
        if mss <= 0:
            raise ValueError("mss must be positive")
        if rto <= 0:
            raise ValueError("rto must be positive")
        if duplicate_ack_threshold <= 0:
            raise ValueError("duplicate_ack_threshold must be positive")

        if win_size is not None:
            max_segments = win_size
        if max_segments <= 0:
            raise ValueError("max_segments must be positive")

        self.snd_base = initial_seq
        self.next_seq = initial_seq
        self.highest_sent = initial_seq

        self.MSS = mss
        self.mss = mss
        self.N = max_segments
        # rwnd ban đầu: nếu không chỉ định, mặc định bằng kích thước bộ đệm (số gói)
        self.rwnd = rwnd if rwnd is not None else max_segments
        self.rto = rto
        self.duplicate_ack_threshold = duplicate_ack_threshold
        self.dup_ack_count = 0

        # Lưu trữ các gói tin: seq -> dict(payload, is_acked, sent_time, retries)
        self.send_buffer: Dict[int, Dict[str, Any]] = {}
        # Timers: seq -> timestamp hết hạn
        self.timers: Dict[int, float] = {}
        # Hàng đợi các seq đã nạp vào buffer nhưng chưa phát ra mạng
        self._pending: List[int] = []
        # Tổng số bytes đang bay ra mạng chưa được cumulative ACK
        self.InFlight = 0
        # Bộ theo dõi Duplicate ACKs: unacked_seq -> số lượng dup ACK
        self.out_of_order_tracker: Dict[int, int] = {}

    @property
    def seq_to_send(self) -> int:
        """Byte Sequence Number tiếp theo sẵn sàng phát ra mạng."""
        if self._pending:
            return self._pending[0]
        return self.next_seq

    def push_data(self, payload: bytes) -> int:
        """
        Tiếp nhận chuỗi byte từ tầng ứng dụng, phân đoạn thành các khối MSS
        và nạp vào send_buffer.
        
        Returns:
            int: Số lượng gói tin (segments) mới được nạp vào bộ đệm.
        """
        if not isinstance(payload, bytes):
            raise TypeError("payload must be bytes")

        free_slots = self.N - len(self.send_buffer)
        if not payload or free_slots <= 0:
            return 0

        accepted_bytes = 0
        offset = 0
        pushed_segments = 0

        while offset < len(payload) and free_slots > 0:
            chunk = payload[offset:offset + self.MSS]
            seq = self.next_seq

            self.send_buffer[seq] = {
                "payload": chunk,
                "length": len(chunk),
                "is_acked": False,
                "sent_time": None,
                "retries": 0,
            }
            self._pending.append(seq)

            self.next_seq += len(chunk)
            accepted_bytes += len(chunk)
            offset += len(chunk)
            free_slots -= 1
            pushed_segments += 1

        return pushed_segments

    def can_send(self, cwnd: float) -> bool:
        """
        Kiểm tra Cổng kiểm soát phát gói (Transmission Gate).
        Gói tin chỉ được phát khi thỏa mãn đồng thời:
        1. Có gói tin đang chờ gửi trong _pending
        2. Không vượt quá giới hạn cửa sổ bộ đệm N (seq - snd_base < N * MSS)
        3. Không vượt quá cửa sổ nhận của đối tác (rwnd)
        4. Không vượt quá cửa sổ nghẽn (cwnd)
        
        Hỗ trợ cwnd & rwnd được truyền vào theo cả đơn vị số gói (packet) lẫn byte.
        """
        if cwnd < 0:
            raise ValueError("cwnd must be non-negative")
        if not self._pending:
            return False

        seq = self._pending[0]
        size = len(self.send_buffer[seq]["payload"])

        # Chuẩn hoá cwnd sang byte nếu cwnd truyền vào là số lượng gói (cwnd < 500)
        cwnd_bytes = cwnd * self.MSS if cwnd < 500 else cwnd

        # Chuẩn hoá rwnd sang byte nếu rwnd truyền vào là số lượng gói (rwnd <= self.N * 2)
        rwnd_bytes = self.rwnd * self.MSS if self.rwnd <= self.N * 2 else self.rwnd

        within_send_window = (seq - self.snd_base) < (self.N * self.MSS)
        within_flow_window = (self.InFlight + size) <= rwnd_bytes
        within_cong_window = (self.InFlight + size) <= cwnd_bytes

        return within_send_window and within_flow_window and within_cong_window

    def get_next_packet_to_send(self, now: Optional[float] = None) -> Optional[Tuple[int, bytes]]:
        """
        Trích xuất gói tin tiếp theo cần phát ra mạng.
        Tự động kích hoạt timer và cập nhật trạng thái InFlight.
        
        Returns:
            tuple (seq, payload) hoặc None nếu hàng đợi pending trống.
        """
        if not self._pending:
            return None

        seq = self._pending[0]
        entry = self.send_buffer.get(seq)
        if entry is None:
            self._pending.pop(0)
            return None

        # Tự động đánh dấu đã phát ra mạng
        self.mark_packet_sent(seq, now=now)
        return seq, entry["payload"]

    def mark_packet_sent(self, seq: int, now: Optional[float] = None) -> bool:
        """
        Cập nhật trạng thái sau khi một gói tin được phát ra mạng.
        Tránh đếm trùng InFlight nếu được gọi nhiều lần.
        """
        entry = self.send_buffer.get(seq)
        if entry is None:
            return False

        current_time = time.time() if now is None else now

        if self._pending and self._pending[0] == seq:
            self._pending.pop(0)

        # Chỉ tăng InFlight nếu đây là lần phát đầu tiên của segment này
        if entry.get("sent_time") is None:
            self.InFlight += len(entry["payload"])

        entry["sent_time"] = current_time
        entry["retries"] = entry.get("retries", 0) + 1
        self.timers[seq] = current_time + self.rto

        self.highest_sent = max(
            self.highest_sent,
            seq + len(entry["payload"]),
        )
        return True

    def on_ack_received(self, ack_seq: int, rcv_rwnd: int) -> Tuple[bool, Optional[int]]:
        """
        Xử lý khi nhận được gói tin ACK từ bên nhận (Cumulative ACK & DupACK tracking).
        
        Returns:
            tuple (is_new_ack: bool, loss_seq: Optional[int]):
                - is_new_ack: True nếu ACK xác nhận thêm byte mới.
                - loss_seq: Số hiệu gói bị nghi mất nếu đạt ngưỡng DUP_ACK_THRESHOLD.
        """
        if rcv_rwnd < 0:
            raise ValueError("rcv_rwnd must be non-negative")

        # Bỏ qua ACK cũ hoặc ACK vượt quá sequence đã phát
        if ack_seq < self.snd_base or ack_seq > self.highest_sent:
            return False, None

        window_changed = (rcv_rwnd != self.rwnd)
        self.rwnd = rcv_rwnd

        # 1. Duplicate ACK: Bên nhận vẫn đang chờ byte tại snd_base
        if ack_seq == self.snd_base:
            if window_changed:
                self.out_of_order_tracker.pop(ack_seq, None)
                self.dup_ack_count = 0
                return False, None

            later_segment_in_flight = any(
                seq > ack_seq and entry.get("sent_time") is not None
                for seq, entry in self.send_buffer.items()
            )
            if not later_segment_in_flight:
                return False, None

            count = self.out_of_order_tracker.get(ack_seq, 0) + 1
            self.out_of_order_tracker[ack_seq] = count
            self.dup_ack_count = count

            if count == self.duplicate_ack_threshold:
                return False, ack_seq
            return False, None

        # 2. New ACK: Xác nhận dữ liệu mới (ack_seq > snd_base)
        acknowledged_bytes = ack_seq - self.snd_base
        self.dup_ack_count = 0
        new_buffer = {}
        new_timers = {}
        new_pending = []

        for seq, entry in sorted(self.send_buffer.items()):
            payload = entry["payload"]
            end_seq = seq + len(payload)

            # Gói tin đã được cumulative ACK trọn vẹn
            if end_seq <= ack_seq:
                continue

            # ACK bao phủ một phần đầu của gói: cắt bớt phần đã ACK
            if seq < ack_seq < end_seq:
                trim = ack_seq - seq
                remaining = payload[trim:]
                new_seq = ack_seq

                updated = dict(entry)
                updated["payload"] = remaining
                updated["length"] = len(remaining)
                updated["is_acked"] = False
                new_buffer[new_seq] = updated

                if seq in self.timers:
                    new_timers[new_seq] = self.timers[seq]
                if seq in self._pending:
                    new_pending.append(new_seq)
                continue

            # Gói tin chưa được ACK
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

        # Xóa các bộ đếm dup ACK cho các sequence đã trôi qua
        self.out_of_order_tracker = {
            s: c for s, c in self.out_of_order_tracker.items()
            if s >= self.snd_base
        }

        return True, None

    def get_retransmission(self, seq: int, now: Optional[float] = None) -> Optional[Tuple[int, bytes]]:
        """Lấy dữ liệu gói tin cần truyền lại."""
        entry = self.send_buffer.get(seq)
        if entry is None or entry.get("sent_time") is None:
            return None
        return seq, entry["payload"]

    def mark_retransmission_sent(self, seq: int, now: Optional[float] = None) -> bool:
        """Cập nhật lại timer sau khi phát gói truyền lại."""
        entry = self.send_buffer.get(seq)
        if entry is None or entry.get("sent_time") is None:
            return False

        current_time = time.time() if now is None else now
        entry["sent_time"] = current_time
        entry["retries"] = entry.get("retries", 0) + 1
        self.timers[seq] = current_time + self.rto
        return True

    def check_timeouts(self, now: Optional[float] = None) -> List[Tuple[int, bytes]]:
        """
        Quét danh sách các gói tin đang bay để phát hiện gói bị hết hạn (Timeout).
        Tự động nhân đôi/tăng thời gian chờ (Exponential Backoff) cho gói bị timeout.
        
        Returns:
            list[tuple(seq, payload)]: Danh sách các gói cần truyền lại.
        """
        current_time = time.time() if now is None else now
        timed_out_packets = []

        for seq, deadline in list(self.timers.items()):
            if deadline <= current_time:
                entry = self.send_buffer.get(seq)
                if entry is not None and entry.get("sent_time") is not None:
                    entry["retries"] = entry.get("retries", 0) + 1
                    # Exponential Backoff nhẹ (1.5x RTO)
                    self.timers[seq] = current_time + (self.rto * 1.5)
                    timed_out_packets.append((seq, entry["payload"]))

        return timed_out_packets

    def get_timed_out_packets(self, now: Optional[float] = None) -> List[Tuple[int, bytes]]:
        """Hàm tương thích cho check_timeouts."""
        return self.check_timeouts(now=now)

    def get_inflight(self) -> int:
        """Trả về số lượng gói (segments) đang bay chưa được ACK."""
        return len(self.timers)

    def has_unacked_data(self) -> bool:
        """Kiểm tra xem còn dữ liệu nào chưa được ACK hoàn tất hay không."""
        return len(self.send_buffer) > 0


class ReceiveBufferManager:
    """
    Quản lý bộ đệm nhận (Receive Buffer) và phân hệ Điều khiển luồng (Flow Control) phía Nhận.
    
    Attributes:
        M (int): Dung lượng tối đa của bộ đệm nhận (số gói tin).
        mss (int): Kích thước tối đa mỗi segment.
        rcv_nxt (int): Byte Sequence Number tiếp theo mong đợi gửi ACK ra mạng.
        read_ptr (int): Byte Sequence Number tiếp theo chờ ứng dụng đọc (read_stream).
        occupied (int): Số lượng gói tin hiện đang lưu trong bộ đệm chờ đọc.
        rcv_buffer (dict): Lưu trữ dữ liệu các gói tin nhận được {seq: payload_bytes}.
    """

    def __init__(self, initial_seq: int = 0, win_size: int = constants.RCV_WIN_SIZE, mss: int = constants.MSS):
        """Khởi tạo bộ đệm nhận cho kết nối mới."""
        self.M = win_size
        self.mss = mss
        self.rcv_nxt = initial_seq
        self.read_ptr = initial_seq
        self.occupied = 0
        self.rcv_buffer: Dict[int, bytes] = {}

    @property
    def rcv_base(self) -> int:
        """Thuộc tính tương thích ngược cho read_ptr."""
        return self.read_ptr

    def get_rwnd(self) -> int:
        """
        Tính toán kích thước cửa sổ nhận còn trống hiện thời (tính theo số gói).
        rwnd = M - occupied
        """
        return max(0, self.M - self.occupied)

    def on_packet_arrived(self, seq: int, payload: bytes) -> Tuple[str, int]:
        """
        Xử lý khi một gói tin DATA(seq) cập bến từ card mạng.
        
        Args:
            seq (int): Byte Sequence Number của gói tin nhận được.
            payload (bytes): Dữ liệu tải của gói tin.
            
        Returns:
            tuple (action: str, current_rwnd: int):
                - action: 'ACCEPT', 'DUPLICATE', 'OUT_OF_WINDOW', 'BUFFER_FULL'.
                - current_rwnd: Cửa sổ nhận sau khi xử lý gói tin (tính theo số gói).
        """
        # 1. Kiểm tra gói cũ hoặc trùng lặp
        if seq < self.read_ptr or seq in self.rcv_buffer:
            return constants.ACTION_DUPLICATE, self.get_rwnd()

        # 2. Kiểm tra tràn bộ đệm nhận (Buffer Full)
        if self.occupied >= self.M:
            return constants.ACTION_BUFFER_FULL, 0

        # 3. Kiểm tra gói rơi ra ngoài phạm vi cửa sổ nhận (Out of Window)
        if seq >= (self.read_ptr + self.M * self.mss):
            return constants.ACTION_OUT_OF_WINDOW, self.get_rwnd()

        # 4. Gói tin hợp lệ nằm trong cửa sổ: Lưu vào buffer
        self.rcv_buffer[seq] = payload
        self.occupied += 1

        # Cập nhật rcv_nxt (tiến qua các segment liên tục đã nhận)
        while self.rcv_nxt in self.rcv_buffer:
            self.rcv_nxt += len(self.rcv_buffer[self.rcv_nxt])

        return constants.ACTION_ACCEPT, self.get_rwnd()

    def read_stream(self) -> bytes:
        """
        Rút tuần tự các gói tin liên tục từ read_ptr để bàn giao cho tầng ứng dụng.
        Mỗi gói được rút ra sẽ giải phóng bộ đệm (occupied--) giúp rwnd tăng trở lại.
        
        Returns:
            bytes: Chuỗi byte liên tục sẵn sàng giao cho HTTP Adapter hoặc Chat Engine.
        """
        stream_bytes = b""

        while self.read_ptr in self.rcv_buffer:
            payload = self.rcv_buffer.pop(self.read_ptr)
            stream_bytes += payload
            self.read_ptr += len(payload)
            self.occupied = max(0, self.occupied - 1)

        return stream_bytes

    def get_next_ack(self) -> int:
        """Trả về Sequence Number tiếp theo mà bên nhận mong đợi nhận được (Ack Number)."""
        return self.rcv_nxt
