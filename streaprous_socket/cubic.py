import math
import time

try:
    from .constant import (
        INIT_CWND, CUBIC_C, CUBIC_BETA,
        CC_STATE_SLOW_START, CC_STATE_CUBIC, CC_STATE_FAST_RECOVERY,
    )
except ImportError:
    from constant import (
        INIT_CWND, CUBIC_C, CUBIC_BETA,
        CC_STATE_SLOW_START, CC_STATE_CUBIC, CC_STATE_FAST_RECOVERY,
    )


class CubicCongestionControl:
    """RFC 8312 CUBIC Congestion Control implementation."""

    def __init__(self, *, clock=None):
        self._clock = clock if clock is not None else time.monotonic
        self.cwnd = float(INIT_CWND)
        self.ssthresh = math.inf
        self.W_max = float(INIT_CWND)
        self.state = CC_STATE_SLOW_START
        self.t_epoch = None
        self.K = 0.0
        self.recovery_point = None
        self.recovery_active = False
        self.last_rtt = None

    def get_cwnd(self):
        return max(1, math.floor(self.cwnd))

    def _start_cubic_epoch(self):
        self.t_epoch = self._clock()
        self.K = ((self.W_max * (1.0 - CUBIC_BETA)) / CUBIC_C) ** (1.0 / 3.0)
        self.state = CC_STATE_CUBIC

    def on_ack(self, n_new=1, rtt=None, *, recovery_complete=False, ack_seq=None):
        if type(n_new) is not int or n_new < 0:
            raise ValueError("n_new must be a non-negative integer")
        if rtt is not None:
            if not math.isfinite(rtt) or rtt <= 0:
                raise ValueError("rtt must be positive and finite")
            self.last_rtt = float(rtt)

        if self.state == CC_STATE_FAST_RECOVERY:
            if recovery_complete or (ack_seq is not None and self.recovery_point is not None and ack_seq >= self.recovery_point):
                self.recovery_active = False
                self.recovery_point = None
                self._start_cubic_epoch()
            return self.cwnd

        if n_new == 0:
            return self.cwnd

        if self.state == CC_STATE_SLOW_START:
            if self.cwnd + n_new < self.ssthresh:
                self.cwnd += n_new
                return self.cwnd

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

        # CUBIC congestion avoidance
        effective_rtt = self.last_rtt if self.last_rtt is not None else 0.1
        t = max(0.0, self._clock() - self.t_epoch) if self.t_epoch is not None else 0.0

        w_cubic = CUBIC_C * ((t + effective_rtt - self.K) ** 3) + self.W_max
        w_est = (self.W_max * CUBIC_BETA
                 + 3.0 * (1.0 - CUBIC_BETA) / (1.0 + CUBIC_BETA)
                 * (t / effective_rtt))

        w_target = max(w_cubic, w_est)
        if w_target > self.cwnd:
            increment = n_new * (w_target - self.cwnd) / max(self.cwnd, 1.0)
            self.cwnd = min(w_target, self.cwnd + increment)

        return self.cwnd

    def on_loss(self, highest_seq_sent):
        if type(highest_seq_sent) is not int or highest_seq_sent < 0:
            raise ValueError("highest_seq_sent must be a non-negative integer")

        if self.recovery_active:
            return self.cwnd

        if self.cwnd < self.W_max:
            self.W_max = self.cwnd * (1.0 + CUBIC_BETA) / 2.0
        else:
            self.W_max = self.cwnd

        self.ssthresh = max(CUBIC_BETA * self.cwnd, 2.0)
        self.cwnd = self.ssthresh
        self.recovery_point = highest_seq_sent
        self.recovery_active = True
        self.t_epoch = None
        self.K = 0.0
        self.state = CC_STATE_FAST_RECOVERY
        return self.cwnd

    def on_timeout(self):
        self.W_max = self.cwnd
        self.ssthresh = max(CUBIC_BETA * self.cwnd, 2.0)
        self.cwnd = 1.0
        self.recovery_active = False
        self.recovery_point = None
        self.t_epoch = None
        self.K = 0.0
        self.state = CC_STATE_SLOW_START
        return self.cwnd
