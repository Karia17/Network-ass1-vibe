# -*- coding: utf-8 -*-
"""CUBIC congestion-window controller for the StreapRous transport project.

The controller works in *packets* (one packet is at most MSS payload bytes).
It does not send packets, track timers, detect losses or maintain a send buffer.
Reliability/Core Transport must call the event methods and enforce cwnd/rwnd.

Implementation follows the group's simplified CUBIC state machine (RFC 8312
in the course design), not a claim of full RFC 9438 compliance.  In particular,
recovery completion is explicitly supplied by Selective Repeat Reliability.
"""

from __future__ import annotations

import math
import time
from typing import Callable, Optional

try:
    from .constants import (
        INIT_CWND, CUBIC_C, CUBIC_BETA,
        CC_STATE_SLOW_START, CC_STATE_CUBIC, CC_STATE_FAST_RECOVERY,
    )
except ImportError:
    from constants import (
        INIT_CWND, CUBIC_C, CUBIC_BETA,
        CC_STATE_SLOW_START, CC_STATE_CUBIC, CC_STATE_FAST_RECOVERY,
    )


class CubicCongestionControl:
    """Per-connection congestion-window controller.

    Args:
        clock: Optional monotonic clock in seconds (useful for deterministic tests).

    Public events:
      - on_ack(n_new, rtt, *, recovery_complete=False)
      - on_loss(highest_seq_sent)
      - on_timeout()
      - get_cwnd()

    Important: for Selective Repeat, an ACK for the largest sequence number
    does NOT by itself mean that earlier packets were all acknowledged.  The
    Reliability layer must send recovery_complete=True only when *all* data
    through recovery_point has been cumulatively accounted for.
    """

    def __init__(self, *, clock: Optional[Callable[[], float]] = None) -> None:
        self._clock = clock if clock is not None else time.monotonic
        self.cwnd = float(INIT_CWND)
        self.ssthresh = math.inf
        self.W_max = float(INIT_CWND) #cua so nghen trc tg gan nhat
        self.state = CC_STATE_SLOW_START
        self.t_epoch: Optional[float] = None #tg bd pha CUBIC hien tai
        self.K = 0.0
        self.recovery_point: Optional[int] = None
        self.recovery_active = False
        self.last_rtt: Optional[float] = None #chu ky trc cua cai j ko bt :3

    def get_cwnd(self) -> int:
        """Return full packets permitted by the current congestion window."""
        return max(1, math.floor(self.cwnd))

    def _start_cubic_epoch(self) -> None:
        """Reset time origin and curvature after Slow Start or recovery."""
        self.t_epoch = self._clock()
        # W_max is the point the cubic curve approaches at time K.
        self.K = ((self.W_max * (1.0 - CUBIC_BETA)) / CUBIC_C) ** (1.0 / 3.0)
        self.state = CC_STATE_CUBIC

    def on_ack(
        self,
        n_new: int = 1,
        rtt: Optional[float] = None,
        *,
        recovery_complete: bool = False,
        ack_seq: Optional[int] = None,
    ) -> float:
        """Process newly acknowledged packets and optional recovery completion.

        n_new: distinct *newly* ACKed packets, not count of duplicate ACKs.
        rtt: positive RTT sample in seconds, if one is available.
        recovery_complete: REL-confirmed completion of all packets through the
            saved recovery_point (never infer from a single selective ACK).
        ack_seq: optional cumulative ACK sequence number to automatically complete recovery.

        Returns current floating-point cwnd for logging.
        """
        if not isinstance(n_new, int) or isinstance(n_new, bool) or n_new < 0:
            raise ValueError("n_new must be a non-negative integer")
        if rtt is not None:
            if not math.isfinite(rtt) or rtt <= 0:
                raise ValueError("rtt must be positive and finite (seconds)")
            self.last_rtt = float(rtt)

        if self.state == CC_STATE_FAST_RECOVERY:
            if recovery_complete or (ack_seq is not None and self.recovery_point is not None and ack_seq >= self.recovery_point):
                self.recovery_active = False
                self.recovery_point = None
                self._start_cubic_epoch()
            # The project's Fast Recovery keeps cwnd fixed; no ACK growth here.
            return self.cwnd

        if n_new == 0:
            return self.cwnd  # Window-only / duplicate ACK must not grow cwnd.

        if self.state == CC_STATE_SLOW_START:
            # A newly acknowledged packet contributes one packet of cwnd.
            if self.cwnd + n_new < self.ssthresh:
                self.cwnd += n_new
                return self.cwnd

            # If we cross the threshold on a batched ACK, only the portion up
            # to ssthresh belongs to Slow Start.
            if math.isfinite(self.ssthresh):
                to_threshold = max(0, math.ceil(self.ssthresh - self.cwnd))
                slow_acked = min(n_new, to_threshold)
                self.cwnd += slow_acked
                n_new -= slow_acked
            else:
                self.cwnd += n_new
                return self.cwnd
            if self.cwnd > self.W_max:
                self.W_max = self.cwnd
            self._start_cubic_epoch()
            if n_new == 0:
                return self.cwnd

        # CUBIC congestion avoidance. Use group's ACK-based update formula.
        # Fall back to default estimate (100ms) if no prior RTT sample has been measured yet.
        effective_rtt = self.last_rtt if self.last_rtt is not None else 0.1

        assert self.t_epoch is not None
        t = max(0.0, self._clock() - self.t_epoch)
        w_cubic = CUBIC_C * (t + effective_rtt - self.K) ** 3 + self.W_max
        w_est = (self.W_max * CUBIC_BETA
                 + 3.0 * (1.0 - CUBIC_BETA) / (1.0 + CUBIC_BETA)
                 * t / effective_rtt)
        w_target = max(w_cubic, w_est)
        if w_target > self.cwnd:
            # Equivalent to repeated ACK-driven steps in the group's design,
            # approximated for a batched selective ACK; never jump past target.
            increment = n_new * (w_target - self.cwnd) / max(self.cwnd, 1.0)
            self.cwnd = min(w_target, self.cwnd + increment)
        return self.cwnd

    def on_loss(self, highest_seq_sent: int) -> float:
        """Handle one new loss/recovery episode, reducing cwnd once.

        REL detects losses and handles retransmission. This method only updates
        congestion state. highest_seq_sent is the highest seq sent *at entry*.
        """
        if not isinstance(highest_seq_sent, int) or isinstance(highest_seq_sent, bool) or highest_seq_sent < 0:
            raise ValueError("highest_seq_sent must be a non-negative integer")
        if self.recovery_active:
            return self.cwnd  # Avoid multiple decreases in the same episode.

        before_loss = self.cwnd
        if before_loss < self.W_max:
            # Optional Fast Convergence from group's specification.
            self.W_max = before_loss * (1.0 + CUBIC_BETA) / 2.0
        else:
            self.W_max = before_loss

        self.ssthresh = max(CUBIC_BETA * before_loss, 2.0) #bat buoc phai toi 2 goi (neu ko thi doi 200 ms) thi he dieu hanh linux ms gui ACK
        self.cwnd = self.ssthresh
        self.recovery_point = highest_seq_sent
        self.recovery_active = True
        self.t_epoch = None
        self.K = 0.0
        self.state = CC_STATE_FAST_RECOVERY
        return self.cwnd

    def on_timeout(self) -> float:
        """Timeout reaction in the group's design: reset to Slow Start.

        RTO calculation, backoff and retransmission remain REL's responsibility.
        """
        before_timeout = self.cwnd
        self.W_max = before_timeout
        self.ssthresh = max(CUBIC_BETA * before_timeout, 2.0)
        self.cwnd = 1.0
        self.recovery_active = False
        self.recovery_point = None
        self.t_epoch = None
        self.K = 0.0
        self.state = CC_STATE_SLOW_START
        return self.cwnd
