from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True)
class RetryPolicy:
    """Backoff exponencial com teto e jitter para retries *agendados*.

    Com os defaults (5s, x2, teto 10min, 12 tentativas) o total de espera é
    ~50 min (5+10+20+40+80+160+320+600*4 s): cobre com folga uma indisponibilidade
    de 30 min do serviço externo (Cenário 3) sem martelar o serviço enquanto ele
    se recupera.
    """

    max_attempts: int = 12
    base_delay_seconds: float = 5.0
    multiplier: float = 2.0
    max_delay_seconds: float = 600.0
    jitter_ratio: float = 0.2  # +-20% para evitar thundering herd na volta do serviço

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts deve ser >= 1")
        if not 0 <= self.jitter_ratio < 1:
            raise ValueError("jitter_ratio deve estar em [0, 1)")

    def can_retry(self, attempt: int) -> bool:
        return attempt < self.max_attempts

    def delay_after(self, attempt: int, rng: random.Random | None = None) -> timedelta:
        """Espera antes da tentativa ``attempt + 1``."""
        raw = self.base_delay_seconds * (self.multiplier ** (attempt - 1))
        capped = min(raw, self.max_delay_seconds)
        if self.jitter_ratio:
            r = rng or random
            capped *= 1 + r.uniform(-self.jitter_ratio, self.jitter_ratio)
        return timedelta(seconds=max(capped, 0.0))

    def total_budget(self) -> timedelta:
        """Soma das esperas sem jitter (documentação/observabilidade)."""
        total = sum(
            min(self.base_delay_seconds * self.multiplier ** (a - 1), self.max_delay_seconds)
            for a in range(1, self.max_attempts)
        )
        return timedelta(seconds=total)
