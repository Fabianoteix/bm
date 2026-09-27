"""Métricas Prometheus (RED + sinais específicos de mensageria)."""

from prometheus_client import Counter, Gauge, Histogram

HTTP_REQUESTS = Counter("http_requests_total", "Requisições HTTP", ["method", "route", "status"])
HTTP_LATENCY = Histogram("http_request_duration_seconds", "Latência HTTP", ["method", "route"])

TRANSACTIONS_CREATED = Counter("transactions_created_total", "Transações criadas", ["replayed"])
PROCESSING_OUTCOMES = Counter(
    "transactions_processing_outcomes_total",
    "Resultado de cada mensagem processada",
    ["outcome"],
)
PROCESSING_DURATION = Histogram(
    "transactions_processing_duration_seconds", "Tempo de processamento por mensagem"
)
POISON_MESSAGES = Counter(
    "transactions_poison_messages_total", "Mensagens inválidas enviadas à DLQ", ["reason"]
)
CONSUMER_ERRORS = Counter(
    "transactions_consumer_errors_total", "Erros inesperados no consumer (mensagem não confirmada)"
)
CONSUMER_PAUSED = Gauge(
    "transactions_consumer_paused",
    "1 enquanto o consumo está pausado por backpressure (circuit breaker aberto)",
)

RISK_CALLS = Histogram(
    "risk_service_request_duration_seconds",
    "Latência das chamadas ao serviço de risco",
    ["outcome"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10),
)
CIRCUIT_OPEN = Gauge(
    "risk_service_circuit_open",
    "1 se o circuit breaker do serviço de risco não está fechado (mantida por compatibilidade)",
)
CIRCUIT_STATE = Gauge(
    "risk_service_circuit_state",
    "Estado do circuit breaker do risco neste processo: 0 fechado, 1 meio aberto, 2 aberto",
)
CIRCUIT_TRANSITIONS = Counter(
    "risk_service_circuit_transitions_total",
    "Transições de estado do circuit breaker do serviço de risco",
    ["to_state"],
)

OUTBOX_PUBLISHED = Counter("outbox_published_total", "Eventos publicados pelo relay", ["topic"])
OUTBOX_PUBLISH_FAILURES = Counter("outbox_publish_failures_total", "Falhas de publicação")
OUTBOX_LEASE_RECLAIMED = Counter(
    "outbox_lease_reclaimed_total",
    "Linhas reassumidas após o lease de outro relay vencer (relay morreu ou travou)",
)
OUTBOX_PENDING = Gauge(
    "outbox_pending_events", "Eventos prontos e ainda não publicados (lag do outbox)"
)
OUTBOX_OLDEST_AGE = Gauge(
    "outbox_oldest_pending_age_seconds", "Idade do evento pronto mais antigo não publicado"
)
