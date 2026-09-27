class ApplicationError(Exception):
    """Base para erros da camada de aplicação."""


class TransactionNotFound(ApplicationError):
    pass


class IdempotencyConflict(ApplicationError):
    """Mesma Idempotency-Key reutilizada com payload diferente."""


class DuplicateIdempotencyKey(ApplicationError):
    """Violação da constraint única de idempotency_key (corrida entre requisições)."""


class ConcurrencyConflict(ApplicationError):
    """Lock otimista falhou: outra instância alterou a transação antes."""


class NotReprocessable(ApplicationError):
    """Só transações FAILED podem ser reprocessadas manualmente."""


# ---- serviço externo -------------------------------------------------------
class RiskServiceError(ApplicationError):
    """Erro permanente: repetir não vai ajudar (ex.: 4xx, contrato inválido)."""


class RiskServiceUnavailable(RiskServiceError):
    """Erro transitório: timeout, 5xx, conexão recusada, circuit breaker aberto."""
