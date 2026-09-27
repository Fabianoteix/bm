"""Worker: consome o tópico de processamento e chama o serviço de risco.

Escala horizontalmente até o nº de partições do tópico (uma partição é
consumida por no máximo um worker do grupo).
"""

from __future__ import annotations

import signal
import socket
import threading

import structlog
from confluent_kafka import Consumer
from prometheus_client import start_http_server

from transactions.adapters.messaging.kafka_producer import KafkaPublisher
from transactions.adapters.messaging.processing_consumer import (
    KafkaProcessingConsumer,
    ProcessingMessageHandler,
    consumer_config,
)
from transactions.adapters.risk.circuit_breaker import CircuitState
from transactions.bootstrap import build_container
from transactions.config import get_settings
from transactions.observability.logging import configure_logging

log = structlog.get_logger(__name__)


def install_signal_handlers(stop: threading.Event) -> None:
    def _handler(signum: int, _frame: object) -> None:
        log.info("shutdown.signal_received", signal=signum)
        stop.set()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def main() -> None:
    settings = get_settings()
    configure_logging(
        level=settings.log_level,
        json_output=settings.log_json,
        service=f"{settings.service_name}-worker",
        environment=settings.environment,
    )
    start_http_server(settings.metrics_port)
    container = build_container(settings)

    dlq_publisher = KafkaPublisher.from_config(
        settings.kafka_bootstrap_servers, f"worker-dlq-{socket.gethostname()}"
    )
    breaker = container.risk_circuit_breaker()
    handler = ProcessingMessageHandler(
        container.process_transaction(breaker), dlq_publisher, settings.kafka_dlq_topic
    )
    consumer = Consumer(
        consumer_config(
            settings.kafka_bootstrap_servers,
            settings.kafka_consumer_group,
            settings.kafka_max_poll_interval_ms,
        )
    )
    stop = threading.Event()
    install_signal_handlers(stop)
    log.info(
        "worker.started",
        topic=settings.kafka_processing_topic,
        group=settings.kafka_consumer_group,
        retry_budget_seconds=settings.retry_policy().total_budget().total_seconds(),
    )
    KafkaProcessingConsumer(
        consumer,
        settings.kafka_processing_topic,
        handler,
        poll_timeout=settings.kafka_poll_timeout_seconds,
        # Backpressure: com o serviço de risco fora, não consome (a mensagem espera
        # no Kafka). Após o reset_timeout o breaker vai a HALF_OPEN e o consumo volta:
        # a próxima mensagem é a sonda.
        should_pause=lambda: breaker.state is CircuitState.OPEN,
    ).run(stop)
    dlq_publisher.close()
    log.info("worker.stopped")


if __name__ == "__main__":
    main()
