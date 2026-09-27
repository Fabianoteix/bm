from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

from transactions.application.retry_policy import RetryPolicy


class Settings(BaseSettings):
    """Configuração 12-factor: tudo via variáveis de ambiente (prefixo APP_)."""

    model_config = SettingsConfigDict(env_prefix="APP_", env_file=".env", extra="ignore")

    service_name: str = "transactions"
    environment: str = "local"
    log_level: str = "INFO"
    log_json: bool = True

    # MySQL
    database_url: str = "mysql+pymysql://app:app@localhost:3306/transactions"
    db_pool_size: int = 10
    db_max_overflow: int = 20
    db_pool_recycle_seconds: int = 1800

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_processing_topic: str = "transactions.processing.v1"  # fila de trabalho interna
    kafka_events_topic: str = "transactions.events.v1"  # eventos de integração
    kafka_dlq_topic: str = "transactions.processing.dlq.v1"
    kafka_consumer_group: str = "transaction-processor"
    kafka_poll_timeout_seconds: float = 1.0
    kafka_max_poll_interval_ms: int = 300_000

    # Outbox relay
    outbox_batch_size: int = 500
    outbox_poll_interval_seconds: float = 0.2
    outbox_publish_timeout_seconds: float = 10.0
    # Lease > delivery.timeout.ms do producer (30 s): outro relay só assume a
    # linha quando não há mais chance de a publicação anterior estar em voo.
    outbox_lease_seconds: float = 45.0

    # Serviço de risco
    risk_service_url: str = "http://localhost:8081"
    risk_connect_timeout_seconds: float = 1.0
    risk_read_timeout_seconds: float = 3.0
    risk_inline_retries: int = 2  # retries curtos, dentro da mesma tentativa
    risk_inline_backoff_seconds: float = 0.2
    circuit_failure_threshold: int = 5
    circuit_reset_timeout_seconds: float = 30.0

    # Retry agendado (entre tentativas)
    retry_max_attempts: int = 12
    retry_base_delay_seconds: float = 5.0
    retry_multiplier: float = 2.0
    retry_max_delay_seconds: float = 600.0
    retry_jitter_ratio: float = 0.2

    # Métricas do worker/relay (a API expõe /metrics no próprio servidor HTTP)
    metrics_port: int = 9000

    def retry_policy(self) -> RetryPolicy:
        return RetryPolicy(
            max_attempts=self.retry_max_attempts,
            base_delay_seconds=self.retry_base_delay_seconds,
            multiplier=self.retry_multiplier,
            max_delay_seconds=self.retry_max_delay_seconds,
            jitter_ratio=self.retry_jitter_ratio,
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
