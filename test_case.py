"""Black-box/contract tests for StreapRous CUBIC Congestion Control.

Basis: team congestion-control specification and unified transport flowchart,
NOT the current contents of cubic.py. The tests exercise the *public contract*
(on_ack, on_loss, on_timeout, get_cwnd) and observable state.

Place this file at the project root and run:
    python3 -m unittest -v test_case

Expected project layout (either form works):
    project/streaprous_socket/{__init__.py, constant.py, cubic.py}
    project/test_case.py
OR
    project/{constant.py, cubic.py, test_case.py}

NOTE:
- A failing test means behavior differs from the specified expected result;
  tests are deliberately NOT rewritten merely to match existing code.
- The original flowchart's "ACK >= recovery_point" needs a cumulative ACK
  (highest *contiguously* acknowledged sequence), not a selective ACK.
- Some scenarios are pure integration-contract checks; actual raw-socket
  integration must be tested separately in the group's Core Transport.
- Sequence numbers in examples are PACKET numbers, beginning at 1.
"""

import importlib
import inspect
import math
import sys
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def load_cubic_class():
    """Import from the project's package; support a flat folder for practice."""
    pkg_file = ROOT / "streaprous_socket" / "cubic.py"
    if pkg_file.exists():
        sys.path.insert(0, str(ROOT))
        return importlib.import_module("streaprous_socket.cubic").CubicCongestionControl

    if (ROOT / "cubic.py").exists():
        # A synthetic package lets `from .constant import ...` work unchanged.
        name = "_cubic_contract_package"
        pkg = types.ModuleType(name)
        pkg.__path__ = [str(ROOT)]
        sys.modules.setdefault(name, pkg)
        return importlib.import_module(name + ".cubic").CubicCongestionControl

    raise RuntimeError("Cannot find cubic.py. Put test_case.py in the project root.")


CubicCongestionControl = load_cubic_class()
BETA = 0.7
C = 0.4
INIT_CWND = 10.0


class FakeClock:
    def __init__(self, start=100.0):
        self.now = float(start)

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def cumulative_acked_through(acked_packets, first_seq=1):
    """Highest packet number with NO holes in ACK history."""
    last = first_seq - 1
    while last + 1 in acked_packets:
        last += 1
    return last


def sending_gate(in_flight, rwnd, cwnd, in_sr_window=True):
    """Reference predicate from the unified transport flowchart.

    This tests the documented interface contract, not Core Transport code.
    """
    return (
        in_sr_window
        and in_flight + 1 <= rwnd
        and in_flight + 1 <= math.floor(cwnd)
    )


def ack_event(cc, *, n_new=1, rtt=None, cumulative_ack=None,
              recovery_complete=None):
    """Small interface adapter for the still-undecided ACK API.

    - Prefer cumulative_ack / ack_seq if the implementation exposes one.
    - Otherwise convert the cumulative-ACK condition to recovery_complete.
    - Never use the single largest Selective Repeat ACK as cumulative ACK.
    """
    params = inspect.signature(cc.on_ack).parameters
    args = {"n_new": n_new, "rtt": rtt}
    if recovery_complete is None and cumulative_ack is not None:
        point = cc.recovery_point
        recovery_complete = (point is not None and cumulative_ack >= point)

    if "cumulative_ack" in params and cumulative_ack is not None:
        args["cumulative_ack"] = cumulative_ack
    elif "ack_seq" in params and cumulative_ack is not None:
        args["ack_seq"] = cumulative_ack
    elif "recovery_complete" in params and recovery_complete is not None:
        args["recovery_complete"] = recovery_complete
    elif recovery_complete is not None:
        raise AssertionError(
            "CUBIC.on_ack must accept cumulative_ack, ack_seq (cumulative), "
            "or recovery_complete to implement flowchart Fast Recovery."
        )
    return cc.on_ack(**args)


class CCTest(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.cc = CubicCongestionControl(clock=self.clock)

    def assertWindow(self, value, places=6):
        self.assertAlmostEqual(self.cc.cwnd, value, places=places)

    def enter_recovery(self, cwnd=20, point=100, wmax=None):
        """Arrange pre-loss values, then trigger the *public* loss event."""
        self.cc.cwnd = float(cwnd)
        self.cc.W_max = float(cwnd if wmax is None else wmax)
        self.cc.on_loss(highest_seq_sent=point)

    # A. INITIALIZATION & SLOW START

    def test_CC_01_initialization(self):
        self.assertWindow(INIT_CWND)
        self.assertTrue(math.isinf(self.cc.ssthresh))
        self.assertEqual(self.cc.state, "SLOW_START")

    def test_CC_02_single_new_ack(self):
        self.cc.on_ack(n_new=1, rtt=0.1)
        self.assertWindow(11)

    def test_CC_03_five_new_acks(self):
        self.cc.on_ack(n_new=5, rtt=0.1)
        self.assertWindow(15)

    def test_CC_04_no_new_ack_no_growth(self):
        self.cc.on_ack(n_new=0, rtt=0.1)
        self.assertWindow(10)

    def test_CC_05_infinite_ssthresh_stays_slow_start(self):
        self.cc.on_ack(n_new=15, rtt=0.1)
        self.assertEqual(self.cc.state, "SLOW_START")

    def test_CC_06_reach_threshold_switches_to_cubic(self):
        self.cc.cwnd = 13
        self.cc.ssthresh = 14
        self.cc.on_ack(n_new=1, rtt=0.1)
        self.assertWindow(14)
        self.assertEqual(self.cc.state, "CUBIC")

    # B. LOSS & FAST RECOVERY

    def test_CC_07_first_loss_multiplicative_decrease(self):
        self.enter_recovery(cwnd=20, point=100)
        self.assertWindow(14)
        self.assertAlmostEqual(self.cc.ssthresh, 14)
        self.assertEqual(self.cc.recovery_point, 100)
        self.assertTrue(self.cc.recovery_active)
        self.assertEqual(self.cc.state, "FAST_RECOVERY")

    def test_CC_08_fast_convergence(self):
        self.enter_recovery(cwnd=10, point=100, wmax=20)
        self.assertAlmostEqual(self.cc.W_max, 8.5)
        self.assertWindow(7)

    def test_CC_09_ack_below_recovery_point(self):
        self.enter_recovery()
        ack_event(self.cc, n_new=1, rtt=0.1, cumulative_ack=95)
        self.assertEqual(self.cc.state, "FAST_RECOVERY")
        self.assertWindow(14)

    def test_CC_10_ack_at_recovery_point(self):
        self.enter_recovery()
        ack_event(self.cc, n_new=1, rtt=0.1, cumulative_ack=100)
        self.assertEqual(self.cc.state, "CUBIC")
        self.assertFalse(self.cc.recovery_active)

    def test_CC_11_ack_above_recovery_point(self):
        self.enter_recovery()
        ack_event(self.cc, n_new=1, rtt=0.1, cumulative_ack=101)
        self.assertEqual(self.cc.state, "CUBIC")

    def test_CC_12_second_loss_same_episode_no_second_decrease(self):
        self.enter_recovery()
        self.cc.on_loss(highest_seq_sent=102)
        self.assertWindow(14)
        self.assertEqual(self.cc.recovery_point, 100)

    def test_CC_13_acks_during_incomplete_recovery_do_not_grow(self):
        self.enter_recovery()
        ack_event(self.cc, n_new=3, rtt=0.1, cumulative_ack=97)
        self.assertWindow(14)

    def test_CC_14_new_episode_after_recovery(self):
        self.enter_recovery(cwnd=20, point=100)
        ack_event(self.cc, cumulative_ack=100, rtt=0.1)
        self.cc.on_loss(highest_seq_sent=120)
        self.assertEqual(self.cc.state, "FAST_RECOVERY")
        self.assertEqual(self.cc.recovery_point, 120)
        self.assertAlmostEqual(self.cc.cwnd, 14 * BETA)

    def test_INT_07_selective_ack_is_not_cumulative_ack(self):
        self.enter_recovery(cwnd=20, point=100)
        selectively_acked = set(range(1, 101)) - {95}
        self.assertEqual(cumulative_acked_through(selectively_acked), 94)
        ack_event(self.cc, cumulative_ack=94)
        self.assertEqual(self.cc.state, "FAST_RECOVERY")

    def test_INT_08_gap_filled_completes_recovery(self):
        self.enter_recovery(cwnd=20, point=100)
        selectively_acked = set(range(1, 101)) - {95}
        selectively_acked.add(95)
        cumulative_ack = cumulative_acked_through(selectively_acked)
        self.assertEqual(cumulative_ack, 100)
        ack_event(self.cc, cumulative_ack=cumulative_ack, rtt=0.1)
        self.assertEqual(self.cc.state, "CUBIC")

    # C. CUBIC REFERENCE FORMULAS: independent numerical oracle

    def test_CC_15_epoch_starts_after_recovery(self):
        self.enter_recovery()
        ack_event(self.cc, cumulative_ack=100, rtt=0.1)
        self.assertEqual(self.cc.state, "CUBIC")
        self.assertIsNotNone(self.cc.t_epoch)

    def test_CC_16_K_formula(self):
        self.enter_recovery(cwnd=20)
        ack_event(self.cc, cumulative_ack=100, rtt=0.1)
        expected_k = (20 * (1 - BETA) / C) ** (1 / 3)
        self.assertAlmostEqual(self.cc.K, expected_k, places=6)

    def test_CC_17_W_cubic_reference(self):
        wmax, rtt, t = 20.0, 0.1, 1.0
        k = (wmax * (1 - BETA) / C) ** (1 / 3)
        actual = C * (t + rtt - k) ** 3 + wmax
        self.assertAlmostEqual(actual, 18.98, delta=0.03)

    def test_CC_18_W_est_reference(self):
        wmax, rtt, t = 20.0, 0.1, 1.0
        actual = BETA * wmax + 3 * (1 - BETA) / (1 + BETA) * t / rtt
        self.assertAlmostEqual(actual, 19.294117647058822, places=8)

    def test_CC_19_target_is_max_of_two_curves(self):
        wmax, rtt, t = 20.0, 0.1, 1.0
        k = (wmax * (1 - BETA) / C) ** (1 / 3)
        wc = C * (t + rtt - k) ** 3 + wmax
        we = BETA * wmax + 3 * (1 - BETA) / (1 + BETA) * t / rtt
        self.assertAlmostEqual(max(wc, we), we, places=7)

    def test_CC_20_growth_after_recovery_matches_formula(self):
        self.enter_recovery(cwnd=20, point=100)
        ack_event(self.cc, cumulative_ack=100, n_new=0, rtt=0.1)
        self.clock.advance(1.0)
        previous = self.cc.cwnd  # 14.0
        wmax = self.cc.W_max  # 20.0
        k = (wmax * (1 - BETA) / C) ** (1 / 3)
        wc = C * (1.0 + 0.1 - k) ** 3 + wmax
        we = BETA * wmax + 3 * (1 - BETA) / (1 + BETA) * 1.0 / 0.1
        target = max(wc, we)
        expected = min(target, previous + (target - previous) / previous)
        ack_event(self.cc, n_new=1, rtt=0.1)
        self.assertAlmostEqual(self.cc.cwnd, expected, places=6)

    def test_CC_21_target_below_window_no_growth(self):
        self.enter_recovery(cwnd=20, point=100)
        ack_event(self.cc, cumulative_ack=100, n_new=0, rtt=0.1)
        self.cc.cwnd = 25.0  # arrange target < current cwnd
        ack_event(self.cc, n_new=1, rtt=0.1)
        self.assertWindow(25.0)

    def test_CC_22_duplicate_ack_does_not_grow_cubic(self):
        self.enter_recovery(cwnd=20)
        ack_event(self.cc, cumulative_ack=100, n_new=0, rtt=0.1)
        self.clock.advance(1)
        before = self.cc.cwnd
        ack_event(self.cc, n_new=0, rtt=0.1)
        self.assertWindow(before)

    # D. TIMEOUT

    def test_CC_23_timeout_during_slow_start(self):
        self.cc.on_timeout()
        self.assertWindow(1)
        self.assertAlmostEqual(self.cc.ssthresh, 7)
        self.assertEqual(self.cc.state, "SLOW_START")

    def test_CC_24_timeout_during_cubic(self):
        self.enter_recovery(cwnd=20)
        ack_event(self.cc, cumulative_ack=100, rtt=0.1)
        self.cc.cwnd = 20
        self.cc.on_timeout()
        self.assertWindow(1)
        self.assertAlmostEqual(self.cc.ssthresh, 14)
        self.assertAlmostEqual(self.cc.W_max, 20)
        self.assertEqual(self.cc.state, "SLOW_START")

    def test_CC_25_timeout_during_recovery(self):
        self.enter_recovery(cwnd=20)
        self.cc.on_timeout()
        self.assertWindow(1)
        self.assertEqual(self.cc.state, "SLOW_START")
        self.assertFalse(self.cc.recovery_active)
        self.assertIsNone(self.cc.recovery_point)

    def test_CC_26_new_ack_after_timeout(self):
        self.cc.on_timeout()
        self.cc.on_ack(n_new=1, rtt=0.1)
        self.assertWindow(2)
        self.assertEqual(self.cc.state, "SLOW_START")

    # E. SENDER GATE CONTRACT (NOT the Core Transport implementation)

    def test_INT_01_can_send_if_both_windows_allow(self):
        self.assertTrue(sending_gate(in_flight=9, rwnd=32, cwnd=10))

    def test_INT_02_cwnd_full_blocks(self):
        self.assertFalse(sending_gate(in_flight=10, rwnd=32, cwnd=10))

    def test_INT_03_rwnd_full_blocks(self):
        self.assertFalse(sending_gate(in_flight=5, rwnd=5, cwnd=20))

    def test_INT_04_fractional_cwnd_floored(self):
        self.assertFalse(sending_gate(in_flight=10, rwnd=32, cwnd=10.8))

    def test_INT_05_ack_releases_inflight_capacity(self):
        self.assertFalse(sending_gate(in_flight=10, rwnd=32, cwnd=10))
        self.assertTrue(sending_gate(in_flight=9, rwnd=32, cwnd=10))

    def test_INT_09_loss_reduces_new_send_allowance(self):
        self.enter_recovery(cwnd=20)
        self.assertFalse(sending_gate(in_flight=20, rwnd=32, cwnd=self.cc.get_cwnd()))
        self.assertTrue(sending_gate(in_flight=13, rwnd=32, cwnd=self.cc.get_cwnd()))

    def test_INT_SR_window_must_also_allow(self):
        self.assertFalse(sending_gate(in_flight=0, rwnd=32, cwnd=10,
                                      in_sr_window=False))


if __name__ == "__main__":
    unittest.main(verbosity=2)
