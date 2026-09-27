"""CLI operacional: reprocessamento e housekeeping.

Exemplos::

    transactions-admin reprocess 550e8400-e29b-41d4-a716-446655440000
    transactions-admin reprocess-failed --limit 500
    transactions-admin purge-outbox --days 7
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import UTC, datetime, timedelta

from transactions.adapters.messaging.outbox_relay import OutboxRelay
from transactions.application.errors import NotReprocessable, TransactionNotFound
from transactions.bootstrap import build_container
from transactions.config import get_settings
from transactions.observability.logging import configure_logging


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="transactions-admin")
    sub = parser.add_subparsers(dest="command", required=True)

    p_one = sub.add_parser("reprocess", help="Reprocessa transações FAILED por id")
    p_one.add_argument("ids", nargs="+", type=uuid.UUID)

    p_all = sub.add_parser(
        "reprocess-failed", help="Reprocessa as N transações FAILED mais antigas"
    )
    p_all.add_argument("--limit", type=int, default=100)

    p_purge = sub.add_parser("purge-outbox", help="Remove eventos já publicados do outbox")
    p_purge.add_argument("--days", type=int, default=7)

    args = parser.parse_args(argv)
    settings = get_settings()
    configure_logging(
        level=settings.log_level,
        json_output=settings.log_json,
        service=f"{settings.service_name}-admin",
        environment=settings.environment,
    )
    container = build_container(settings)

    if args.command == "reprocess":
        use_case = container.reprocess_transaction()
        exit_code = 0
        for tx_id in args.ids:
            try:
                use_case.execute(tx_id)
                print(f"OK       {tx_id}")
            except (TransactionNotFound, NotReprocessable) as exc:
                print(f"IGNORED  {tx_id}: {exc}", file=sys.stderr)
                exit_code = 1
        return exit_code

    if args.command == "reprocess-failed":
        ids = container.reprocess_transaction().execute_all_failed(args.limit)
        print(f"{len(ids)} transações reenviadas para processamento")
        return 0

    if args.command == "purge-outbox":
        relay = OutboxRelay(container.session_factory, publisher=None)  # type: ignore[arg-type]
        removed = relay.purge_published(datetime.now(UTC) - timedelta(days=args.days))
        print(f"{removed} eventos removidos do outbox")
        return 0
    return 2  # pragma: no cover


if __name__ == "__main__":
    raise SystemExit(main())
