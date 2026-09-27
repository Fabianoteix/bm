"""Telemetria do circuit breaker: estado por processo visível em métrica e log."""

from __future__ import annotations

from prometheus_client import REGISTRY
from structlog.testing import capture_logs

from transactions.adapters.risk.circuit_breaker import CircuitBreaker, CircuitState
from transactions.observability.circuit_breaker import CircuitBreakerTelemetry


def _state() -> float | None:
    return REGISTRY.get_sample_value("risk_service_circuit_state")


def _open() -> float | None:
    return REGISTRY.get_sample_value("risk_service_circuit_open")


def _transitions(to_state: str) -> float:
    value = REGISTRY.get_sample_value(
        "risk_service_circuit_transitions_total", {"to_state": to_state}
    )
    return value or 0.0


def test_starts_closed_so_the_series_exists_from_boot() -> None:
    CircuitBreakerTelemetry(host="w1")
    assert _state() == 0
    assert _open() == 0


def test_full_cycle_exports_three_states_and_one_log_per_transition() -> None:
    now = [0.0]
    telemetry = CircuitBreakerTelemetry(host="worker-7")
    breaker = CircuitBreaker(
        failure_threshold=2, reset_timeout=10, clock=lambda: now[0], on_state_change=telemetry
    )
    opened_before = _transitions("open")

    with capture_logs() as logs:
        breaker.record_failure()
        breaker.record_failure()
        assert _state() == 2  # aberto
        assert _open() == 1

        now[0] = 10.0
        assert breaker.state is CircuitState.HALF_OPEN
        assert _state() == 1  # meio aberto
        assert _open() == 1  # compatível com a métrica antiga

        breaker.record_success()
        assert _state() == 0  # fechado
        assert _open() == 0

    assert _transitions("open") == opened_before + 1
    transitions = [(e["from_state"], e["to_state"]) for e in logs]
    assert transitions == [("closed", "open"), ("open", "half_open"), ("half_open", "closed")]
    assert all(e["event"] == "circuit.state_changed" for e in logs)
    assert all(e["host"] == "worker-7" and e["dependency"] == "risk_service" for e in logs)
    assert [e["log_level"] for e in logs] == ["warning", "info", "info"]


def test_no_log_when_state_does_not_change() -> None:
    telemetry = CircuitBreakerTelemetry(host="w1")
    breaker = CircuitBreaker(failure_threshold=5, on_state_change=telemetry)
    with capture_logs() as logs:
        breaker.record_success()  # já estava fechado
        breaker.record_failure()  # 1 de 5: continua fechado
    assert logs == []
