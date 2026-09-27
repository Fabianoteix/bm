"""Logs estruturados (JSON) com contexto propagado via contextvars.

Todo log emitido durante uma requisição ou o processamento de uma mensagem
carrega automaticamente ``correlation_id`` e ``transaction_id`` (quando
conhecidos), permitindo seguir uma transação da API ao worker e à DLQ.
"""

from __future__ import annotations

import logging
import sys

import structlog


def configure_logging(
    *,
    level: str = "INFO",
    json_output: bool = True,
    service: str = "transactions",
    environment: str = "local",
) -> None:
    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    shared: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        timestamper,
        _static_fields(service, environment),
    ]
    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer() if json_output else structlog.dev.ConsoleRenderer()
    )

    structlog.configure(
        processors=[
            *shared,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, renderer],
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level.upper())
    for noisy in ("httpx", "httpcore", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def _static_fields(service: str, environment: str) -> structlog.types.Processor:
    def processor(_logger: object, _name: str, event_dict: dict[str, object]) -> dict[str, object]:
        event_dict.setdefault("service", service)
        event_dict.setdefault("env", environment)
        return event_dict

    return processor  # type: ignore[return-value]
