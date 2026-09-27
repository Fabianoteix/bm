import random
from datetime import timedelta

import pytest

from transactions.adapters.risk.circuit_breaker import CircuitBreaker, CircuitState
from transactions.application.retry_policy import RetryPolicy


class TestRetryPolicy:
    def test_exponential_backoff_with_cap(self) -> None:
        p = RetryPolicy(max_attempts=10, base_delay_seconds=5, max_delay_seconds=60, jitter_ratio=0)
        delays = [p.delay_after(a).total_seconds() for a in range(1, 7)]
        assert delays == [5, 10, 20, 40, 60, 60]

    def test_jitter_stays_within_bounds(self) -> None:
        p = RetryPolicy(base_delay_seconds=10, jitter_ratio=0.2)
        rng = random.Random(42)
        for _ in range(200):
            assert 8 <= p.delay_after(1, rng).total_seconds() <= 12

    def test_can_retry_until_max_attempts(self) -> None:
        p = RetryPolicy(max_attempts=3)
        assert [p.can_retry(a) for a in (1, 2, 3)] == [True, True, False]

    def test_default_budget_survives_30_minute_outage(self) -> None:
        """Cenário 3: a janela total de retry precisa ser > 30 min."""
        assert RetryPolicy().total_budget() > timedelta(minutes=30)

    def test_invalid_configuration(self) -> None:
        with pytest.raises(ValueError):
            RetryPolicy(max_attempts=0)


class TestCircuitBreaker:
    def _breaker(self) -> tuple[CircuitBreaker, list[float], list[CircuitState]]:
        now = [0.0]
        changes: list[CircuitState] = []
        cb = CircuitBreaker(
            failure_threshold=3,
            reset_timeout=10,
            clock=lambda: now[0],
            on_state_change=changes.append,
        )
        return cb, now, changes

    def test_opens_after_threshold_and_fails_fast(self) -> None:
        cb, _, _ = self._breaker()
        for _ in range(3):
            assert cb.allow_request()
            cb.record_failure()
        assert cb.state is CircuitState.OPEN
        assert not cb.allow_request()

    def test_half_open_allows_single_probe_then_closes_on_success(self) -> None:
        cb, now, changes = self._breaker()
        for _ in range(3):
            cb.record_failure()
        now[0] = 10.0
        assert cb.allow_request()  # sonda
        assert not cb.allow_request()  # só uma sonda por vez
        cb.record_success()
        assert cb.state is CircuitState.CLOSED
        assert changes == [CircuitState.OPEN, CircuitState.HALF_OPEN, CircuitState.CLOSED]

    def test_half_open_failure_reopens(self) -> None:
        cb, now, _ = self._breaker()
        for _ in range(3):
            cb.record_failure()
        now[0] = 10.0
        assert cb.allow_request()
        cb.record_failure()
        assert cb.state is CircuitState.OPEN

    def test_success_resets_consecutive_failures(self) -> None:
        cb, _, _ = self._breaker()
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        cb.record_failure()
        assert cb.state is CircuitState.CLOSED
