from __future__ import annotations

import socket
import threading

import structlog
from prometheus_client import start_http_server

from transactions.adapters.messaging.kafka_producer import KafkaPublisher
from transactions.adapters.messaging.outbox_relay import OutboxRelay
from transactions.bootstrap import build_container
from transactions.config import get_settings
from transactions.entrypoints.worker import install_signal_handlers
from transactions.observability.logging import configure_logging

log = structlog.get_logger(__name__)


def main() -> None:
    settings = get_settings()
    configure_logging(
        level=settings.log_level,
        json_output=settings.log_json,
        service=f"{settings.service_name}-relay",
        environment=settings.environment,
    )
    start_http_server(settings.metrics_port)
    container = build_container(settings)
    publisher = KafkaPublisher.from_config(
        settings.kafka_bootstrap_servers, f"outbox-relay-{socket.gethostname()}"
    )
    relay = OutboxRelay(
        container.session_factory,
        publisher,
        batch_size=settings.outbox_batch_size,
        publish_timeout=settings.outbox_publish_timeout_seconds,
        lease_seconds=settings.outbox_lease_seconds,
    )
    stop = threading.Event()
    install_signal_handlers(stop)
    log.info(
        "relay.started",
        batch_size=settings.outbox_batch_size,
        lease_seconds=settings.outbox_lease_seconds,
    )
    relay.run_forever(stop, idle_sleep=settings.outbox_poll_interval_seconds)
    publisher.close()
    log.info("relay.stopped")


if __name__ == "__main__":
    main()
