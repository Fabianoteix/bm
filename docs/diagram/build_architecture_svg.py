"""Gera docs/architecture.svg (desenho do System Design).

Layout feito à mão (coordenadas explícitas) para ficar legível — layouts
automáticos embaralham um diagrama com este número de conexões.
Uso: python docs/diagram/build_architecture_svg.py
"""

from __future__ import annotations

import textwrap
from pathlib import Path
from xml.sax.saxutils import escape

W, H = 1640, 1090
OUT = Path(__file__).resolve().parents[1] / "architecture.svg"

C = {
    "ext": ("#fff4e5", "#c77700"),
    "app": ("#e8f1ff", "#2563eb"),
    "store": ("#eafaf0", "#15803d"),
    "topic": ("#f3e8ff", "#7e22ce"),
    "ops": ("#f1f5f9", "#64748b"),
    "group": ("#fbfcfe", "#94a3b8"),
    "fail": ("#fff1f2", "#be123c"),
}

parts: list[str] = []


def box(x, y, w, h, kind, title, lines=(), r=10, dash=False, title_size=14):
    fill, stroke = C[kind]
    d = ' stroke-dasharray="5 4"' if dash else ""
    parts.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill}" '
        f'stroke="{stroke}" stroke-width="1.6"{d}/>'
    )
    cy = y + 22
    parts.append(
        f'<text x="{x + w / 2}" y="{cy}" class="t" font-size="{title_size}" '
        f'text-anchor="middle" fill="{stroke}">{escape(title)}</text>'
    )
    for i, line in enumerate(lines):
        parts.append(
            f'<text x="{x + w / 2}" y="{cy + 19 + i * 16}" class="b" text-anchor="middle">'
            f"{escape(line)}</text>"
        )


def group(x, y, w, h, title):
    fill, stroke = C["group"]
    parts.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" fill="{fill}" '
        f'stroke="{stroke}" stroke-width="1.2" stroke-dasharray="6 4"/>'
    )
    parts.append(f'<text x="{x + 14}" y="{y + 20}" class="g">{escape(title)}</text>')


def arrow(points, label=None, lx=None, ly=None, dashed=False, color="#334155", anchor="middle"):
    d = "M " + " L ".join(f"{px} {py}" for px, py in points)
    dash = ' stroke-dasharray="5 4"' if dashed else ""
    parts.append(
        f'<path d="{d}" fill="none" stroke="{color}" stroke-width="1.6"{dash} '
        f'marker-end="url(#arrow)"/>'
    )
    if label:
        lines = label.split("\n")
        for i, line in enumerate(lines):
            parts.append(
                f'<text x="{lx}" y="{ly + i * 14}" class="l" text-anchor="{anchor}">'
                f"{escape(line)}</text>"
            )


def num(n, x, y):
    parts.append(f'<circle cx="{x}" cy="{y}" r="11" fill="#0f172a"/>')
    parts.append(f'<text x="{x}" y="{y + 4.5}" class="n" text-anchor="middle">{n}</text>')


def card(x, y, w, h, tag, title, body):
    fill, stroke = C["fail"]
    parts.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{fill}" '
        f'stroke="{stroke}" stroke-width="1.4"/>'
    )
    parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="30" rx="10" fill="{stroke}"/>')
    parts.append(f'<rect x="{x}" y="{y + 18}" width="{w}" height="12" fill="{stroke}"/>')
    parts.append(
        f'<text x="{x + 12}" y="{y + 20}" class="ct">{escape(tag)} · {escape(title)}</text>'
    )
    yy = y + 50
    for para in body:
        for line in textwrap.wrap(para, 44):
            parts.append(f'<text x="{x + 12}" y="{yy}" class="cb">{escape(line)}</text>')
            yy += 15.5
        yy += 5


# ------------------------------------------------------------------ título
parts.append(
    '<text x="30" y="42" class="h1">BAMAQ Capital · Processamento assíncrono de transações</text>'
)
parts.append(
    '<text x="30" y="66" class="sub">Arquitetura Hexagonal · Transactional Outbox · '
    "at-least-once + consumidores idempotentes · MySQL é a fonte da verdade</text>"
)

# ---------------------------------------------------------------- componentes
box(30, 215, 130, 62, "ext", "Cliente", ["canal / parceiro"])

group(185, 118, 265, 290, "API · FastAPI (N réplicas, stateless)")
box(
    203,
    150,
    229,
    110,
    "app",
    "POST /transactions",
    ["valida payload (Decimal)", "Idempotency-Key opcional", "202 Accepted + Location"],
)
box(
    203,
    278,
    229,
    110,
    "app",
    "GET /transactions/{id}",
    ["leitura direta por PK (sem cache)", "404 Problem+JSON", "X-Correlation-ID"],
)

group(490, 88, 300, 340, "MySQL 8 · fonte da verdade")
box(
    508,
    120,
    264,
    130,
    "store",
    "transactions",
    [
        "id · customer_id · value DECIMAL",
        "status · attempts · last_error",
        "version (lock otimista)",
        "UNIQUE(customer_id, idem_key)",
    ],
)
box(
    508,
    268,
    264,
    140,
    "store",
    "outbox",
    [
        "event_id · event_type · topic",
        "message_key = transaction_id",
        "available_at (retry agendado)",
        "published_at · publish_attempts",
    ],
)

box(
    835,
    268,
    210,
    152,
    "app",
    "Outbox Relay (N)",
    [
        "eventos c/ available_at ≤ agora",
        "1 reserva c/ lease de 45s",
        "2 publica sem transação aberta",
        "producer idempotente · acks=all",
        "3 marca publicado após o ack",
        "relay morreu → lease vence",
    ],
)

group(1090, 88, 320, 385, "Kafka · key = transaction_id")
box(
    1108,
    120,
    284,
    92,
    "topic",
    "transactions.processing.v1",
    ["ProcessingRequested{attempt}", "48 partições · fila de trabalho"],
)
box(
    1108,
    230,
    284,
    92,
    "topic",
    "transactions.events.v1",
    ["TransactionCreated", "TransactionStatusChanged{version}"],
)
box(
    1108,
    360,
    284,
    96,
    "topic",
    "transactions.processing.dlq.v1",
    ["TransactionDeadLettered", "poison messages (payload original", "+ headers com motivo)"],
)

box(1460, 236, 155, 76, "ext", "Outros sistemas", ["consomem status", "dedupe por event_id"])
box(
    1448,
    352,
    180,
    118,
    "ops",
    "transactions-admin",
    [
        "reprocess · reprocess-failed",
        "FAILED → PENDING",
        "+ novo evento via outbox",
        "purge-outbox",
    ],
    dash=True,
)

group(490, 540, 920, 190, "Worker · consumer group (réplicas ≤ nº de partições)")
box(
    1150,
    575,
    240,
    138,
    "app",
    "Kafka Consumer",
    [
        "auto.offset.store = false",
        "offset armazenado só após",
        "COMMIT no MySQL",
        "poison → DLQ · erro infra → seek",
    ],
)
box(
    835,
    575,
    285,
    138,
    "app",
    "ProcessTransaction (caso de uso)",
    [
        "fencing: attempt == attempts + 1",
        "UPDATE … WHERE version = ?",
        "status + eventos no mesmo COMMIT",
        "transitório → RETRYING + retry",
        "permanente/esgotou → FAILED",
    ],
)
box(
    508,
    575,
    297,
    138,
    "app",
    "RiskAnalysis HTTP adapter",
    [
        "timeout connect 1s / read 3s",
        "2 retries curtos c/ jitter",
        "circuit breaker (5 falhas / 30s)",
        "5xx·408·429·timeout = transitório",
        "4xx·contrato inválido = permanente",
    ],
)
box(
    203, 606, 229, 76, "ext", "Serviço de risco", ["POST /risk-analysis", "Idempotency-Key = tx id"]
)

# ------------------------------------------------------------------- setas
arrow([(160, 238), (203, 206)])
num(1, 180, 206)
arrow([(160, 258), (203, 330)], dashed=True, label="polling", lx=95, ly=305)
arrow([(432, 196), (508, 196)])
num(2, 470, 180)
parts.append('<text x="462" y="222" class="l" text-anchor="middle">1 COMMIT:</text>')
parts.append('<text x="462" y="236" class="l" text-anchor="middle">tx + outbox</text>')
arrow([(432, 330), (478, 330), (478, 240), (508, 240)], dashed=True, label="PK", lx=455, ly=322)

arrow([(772, 338), (835, 338)])
num(3, 803, 322)
arrow([(1045, 338), (1090, 338)])
num(4, 1067, 322)

arrow([(1392, 166), (1432, 166), (1432, 644), (1390, 644)])
num(5, 1432, 520)
arrow([(1150, 644), (1120, 644)])
arrow([(835, 644), (805, 644)])
num(6, 820, 628)
arrow([(508, 644), (432, 644)])

arrow([(930, 575), (930, 505), (740, 505), (740, 408)])
num(7, 930, 520)
parts.append('<text x="836" y="478" class="l" text-anchor="middle">UPDATE status + outbox</text>')
parts.append(
    '<text x="836" y="492" class="l" text-anchor="middle">(StatusChanged · retry · DLQ)</text>'
)
arrow(
    [(1250, 575), (1250, 456)],
    dashed=True,
    color="#be123c",
    label="poison / órfã",
    lx=1258,
    ly=520,
    anchor="start",
)

arrow([(1392, 274), (1460, 274)])
num(8, 1426, 258)
arrow([(1392, 408), (1448, 408)], dashed=True)

# ---------------------------------------------------------- cenários de falha
parts.append('<text x="30" y="775" class="h2">Cenários de falha</text>')
cw, gap, cy = 306, 14, 790
cards = [
    (
        "C1",
        "Kafka fora após commit",
        [
            "O evento já está no outbox, gravado no MESMO COMMIT da transação. "
            "Não existe dual write.",
            "Relay re-tenta com backoff; nada se perde. A API continua aceitando "
            "(readiness não depende do Kafka).",
            "Alerta: outbox_oldest_pending_age_seconds.",
        ],
    ),
    (
        "C2",
        "Crash antes do commit do offset",
        [
            "Offset só é armazenado depois do COMMIT no MySQL → a mensagem é "
            "reentregue (at-least-once).",
            "Fencing: status final ou attempt ≠ attempts+1 → DUPLICATE, ack sem efeito colateral.",
        ],
    ),
    (
        "C3",
        "Risco indisponível por 30 min",
        [
            "Timeouts curtos + 2 retries rápidos; circuit breaker abre e passa a falhar rápido.",
            "Retry agendado = outbox.available_at: 5s·10s·20s…10min (±20% jitter), "
            "12 tentativas ≈ 50 min. Partição não trava.",
            "Esgotou → FAILED + DLQ → reprocess.",
        ],
    ),
    (
        "C4",
        "Mensagem duplicada",
        [
            "Tentativa já consumida ou status final → descartada.",
            "Corrida em rebalance: UPDATE … WHERE version=? (só um vence).",
            "Idempotency-Key no POST (UNIQUE) e na chamada ao risco; consumidores "
            "externos usam event_id + version.",
        ],
    ),
    (
        "C5",
        "100 → 10.000 eventos/min",
        [
            "~167/s. API, relay e worker são stateless → escala horizontal.",
            "48 partições desde o dia 1: escalar = subir workers, sem reparticionar.",
            "Gargalo real: latência do risco → concorrência por worker, pool HTTP, "
            "alerta de consumer lag.",
        ],
    ),
]
for i, (tag, title, body) in enumerate(cards):
    card(30 + i * (cw + gap), cy, cw, 185, tag, title, body)

# ------------------------------------------------------------ observabilidade
box(
    30,
    995,
    1580,
    70,
    "ops",
    "Observabilidade",
    [
        "Logs JSON (structlog) com correlation_id · transaction_id · event_id · partition/offset   ·   "
        "Prometheus: HTTP RED, outcomes por tipo, retries, DLQ, lag do outbox, latência do risco, "
        "estado do breaker POR worker (0/1/2), consumer pausado",
        "correlation_id propagado HTTP → outbox → headers Kafka → worker → chamada ao risco   ·   "
        "alertas versionados (ops/alerts.yml, testados com promtool) · painel Grafana por worker",
    ],
    dash=True,
)

# -------------------------------------------------------------------- legenda
lx0 = 1010
for i, (kind, label) in enumerate(
    [
        ("app", "processo da aplicação"),
        ("store", "armazenamento"),
        ("topic", "tópico Kafka"),
        ("ext", "externo"),
        ("ops", "operação"),
    ]
):
    fill, stroke = C[kind]
    x = lx0 + (i % 3) * 205
    y = 26 + (i // 3) * 22
    parts.append(
        f'<rect x="{x}" y="{y}" width="14" height="14" rx="3" fill="{fill}" stroke="{stroke}"/>'
    )
    parts.append(f'<text x="{x + 20}" y="{y + 11.5}" class="lg">{label}</text>')

style = """
<style>
  text { font-family: Inter, 'Segoe UI', Helvetica, Arial, sans-serif; fill: #0f172a; }
  .h1 { font-size: 24px; font-weight: 700; }
  .h2 { font-size: 18px; font-weight: 700; }
  .sub { font-size: 13.5px; fill: #475569; }
  .t { font-weight: 700; }
  .b { font-size: 12.5px; fill: #1e293b; }
  .g { font-size: 12.5px; font-weight: 600; fill: #475569; }
  .l { font-size: 12px; fill: #334155; font-style: italic; }
  .n { font-size: 12px; font-weight: 700; fill: #ffffff; }
  .ct { font-size: 13px; font-weight: 700; fill: #ffffff; }
  .cb { font-size: 12.5px; fill: #1f2937; }
  .lg { font-size: 12px; fill: #334155; }
</style>
"""
defs = (
    '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" '
    'markerHeight="7" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" '
    'fill="#334155"/></marker></defs>'
)
svg = (
    f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">'
    f"{style}{defs}"
    f'<rect width="{W}" height="{H}" fill="#ffffff"/>' + "".join(parts) + "</svg>"
)
OUT.write_text(svg, encoding="utf-8")
print(f"escrito {OUT}")
