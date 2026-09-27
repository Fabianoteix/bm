from __future__ import annotations

import threading
import time
from collections.abc import Callable
from enum import StrEnum


class CircuitState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker simples (thread-safe), por processo.

    * CLOSED: chamadas passam; ``failure_threshold`` falhas consecutivas → OPEN.
    * OPEN: chamadas falham imediatamente (fail fast) por ``reset_timeout`` s,
      poupando o serviço externo e os workers (sem esperar timeouts).
    * HALF_OPEN: uma chamada de teste; sucesso → CLOSED, falha → OPEN de novo.

    Trade-off: o estado é local a cada worker. Com N workers o serviço recebe até
    N chamadas de teste por janela — aceitável aqui. Se virar problema, o estado
    pode ser compartilhado (tabela no MySQL ou um store como Redis), mas só com
    métrica mostrando a necessidade.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        reset_timeout: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
        on_state_change: Callable[[CircuitState], None] | None = None,
    ) -> None:
        self._threshold = failure_threshold
        self._reset_timeout = reset_timeout
        self._clock = clock
        self._on_state_change = on_state_change
        self._lock = threading.Lock()
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._half_open_in_flight = False

    @property
    def state(self) -> CircuitState:
        with self._lock:
            self._maybe_half_open()
            return self._state

    def allow_request(self) -> bool:
        with self._lock:
            self._maybe_half_open()
            if self._state is CircuitState.CLOSED:
                return True
            if self._state is CircuitState.HALF_OPEN and not self._half_open_in_flight:
                self._half_open_in_flight = True
                return True
            return False

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._half_open_in_flight = False
            self._set(CircuitState.CLOSED)

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            self._half_open_in_flight = False
            if self._state is CircuitState.HALF_OPEN or self._failures >= self._threshold:
                self._opened_at = self._clock()
                self._set(CircuitState.OPEN)

    def _maybe_half_open(self) -> None:
        if (
            self._state is CircuitState.OPEN
            and self._clock() - self._opened_at >= self._reset_timeout
        ):
            self._set(CircuitState.HALF_OPEN)

    def _set(self, state: CircuitState) -> None:
        if state is not self._state:
            self._state = state
            if self._on_state_change:
                self._on_state_change(state)
