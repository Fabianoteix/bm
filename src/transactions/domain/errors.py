class DomainError(Exception):
    """Base para erros de regra de negócio."""


class InvalidTransaction(DomainError):
    """Dados de entrada violam invariantes da transação."""


class InvalidStateTransition(DomainError):
    """Transição de status não permitida pela máquina de estados."""
