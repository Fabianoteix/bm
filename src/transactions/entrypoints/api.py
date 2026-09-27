from __future__ import annotations

import uvicorn
from fastapi import FastAPI

from transactions.adapters.http.app import create_app
from transactions.bootstrap import build_container
from transactions.config import get_settings
from transactions.observability.logging import configure_logging


def build_app() -> FastAPI:
    settings = get_settings()
    configure_logging(
        level=settings.log_level,
        json_output=settings.log_json,
        service=f"{settings.service_name}-api",
        environment=settings.environment,
    )
    container = build_container(settings)
    return create_app(
        container.create_transaction(),
        container.get_transaction(),
        container.readiness_check,
    )


def main() -> None:
    uvicorn.run(
        "transactions.entrypoints.api:build_app",
        factory=True,
        host="0.0.0.0",  # noqa: S104 - dentro do container
        port=8000,
        log_config=None,
        proxy_headers=True,
    )


if __name__ == "__main__":
    main()
