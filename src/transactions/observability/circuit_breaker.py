"""Telemetria do circuit breaker (adapter de observabilidade).

O breaker é local a cada worker, de propósito (sem Redis). O custo disso é que
uma falha parcial fica invisível na taxa de erro agregada: se 3 de 20 workers
abrirem o circuito, a média quase não muda. Este módulo torna o estado de CADA
processo visível:

* gauge ``risk_service_circuit_state`` (0 fechado, 1 meio aberto, 2 aberto). O
  Prometheus coleta cada worker como um alvo próprio, então o label ``instance``
  já separa os processos; ``ops/alerts.yml`` alerta por instância e pela fração
  da frota com o circuito aberto;
* contador de transições (detecta circuito "piscando");
* um log estruturado por transição, com estado anterior e novo e o host.

Fica fora de ``CircuitBreaker`` para a regra de negócio do breaker não conhecer
Prometheus nem logging: é plugado como ``on_state_change`` no composition root.
"""

from __future__ import annotations

import socket

import structlog

from transactions.adapters.risk.circuit_breaker import CircuitState
from transactions.observability import metrics

log = structlog.get_logger(__name__)

STATE_VALUE: dict[CircuitState, int] = {
    CircuitState.CLOSED: 0,
    CircuitState.HALF_OPEN: 1,
    CircuitState.OPEN: 2,
}


class CircuitBreakerTelemetry:
    """Callable para ``CircuitBreaker(on_state_change=...)``."""

    def __init__(self, dependency: str = "risk_service", host: str | None = None) -> None:
        self._dependency = dependency
        self._host = host or socket.gethostname()
        self._previous = CircuitState.CLOSED
        # Publica o estado inicial: a série existe desde o boot (o alerta de
        # "fração da frota aberta" divide pelo total de workers).
        metrics.CIRCUIT_STATE.set(STATE_VALUE[CircuitState.CLOSED])
        metrics.CIRCUIT_OPEN.set(0)

    def __call__(self, state: CircuitState) -> None:
        previous, self._previous = self._previous, state
        metrics.CIRCUIT_STATE.set(STATE_VALUE[state])
        metrics.CIRCUIT_OPEN.set(0 if state is CircuitState.CLOSED else 1)
        metrics.CIRCUIT_TRANSITIONS.labels(to_state=state.value).inc()
        emit = log.warning if state is CircuitState.OPEN else log.info
        emit(
            "circuit.state_changed",
            dependency=self._dependency,
            from_state=previous.value,
            to_state=state.value,
            host=self._host,
        )
