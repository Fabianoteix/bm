# syntax=docker/dockerfile:1.7
# Imagem única para api / worker / relay / admin / risk-mock (muda só o comando).

FROM python:3.12-slim AS builder
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /build
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip wheel --wheel-dir /wheels .

FROM python:3.12-slim AS runtime
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
RUN groupadd --system app && useradd --system --gid app --home /app app
WORKDIR /app
COPY --from=builder /wheels /wheels
RUN pip install /wheels/* && rm -rf /wheels
COPY alembic.ini ./
COPY migrations ./migrations
COPY mock_risk_service ./mock_risk_service
USER app
EXPOSE 8000 9000
CMD ["transactions-api"]
