.PHONY: install lint typecheck test cov alerts-test up up-obs down logs e2e demo-outage reprocess-failed

install:            ## Dependências de desenvolvimento
	pip install -e ".[dev]"

lint:               ## Ruff (lint + format check)
	ruff check . && ruff format --check .

typecheck:          ## mypy --strict
	mypy

test:               ## Unit + integração + API (sem Docker)
	pytest

cov:
	pytest --cov --cov-report=term-missing

alerts-test:        ## Valida e testa as regras de alerta (promtool via Docker)
	docker run --rm -v "$$PWD/ops:/ops" --entrypoint promtool prom/prometheus:v2.54.1 check config /ops/prometheus.yml
	docker run --rm -v "$$PWD/ops:/ops" --entrypoint promtool prom/prometheus:v2.54.1 test rules /ops/alerts_test.yml

up:                 ## Sobe todo o ambiente
	docker compose up -d --build

up-obs:             ## Ambiente + Prometheus (alertas) + Grafana + Kafka UI
	docker compose --profile observability up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f api worker relay

e2e:                ## Testes ponta a ponta contra o compose
	E2E_BASE_URL=http://localhost:8000 E2E_RISK_URL=http://localhost:8081 pytest tests/e2e -m e2e -v

demo-outage:        ## Cenário 3: serviço de risco fora por 30 minutos
	curl -s -X POST localhost:8081/admin/outage -H 'content-type: application/json' -d '{"seconds": 1800}'

reprocess-failed:   ## Reprocessa transações FAILED
	docker compose run --rm api transactions-admin reprocess-failed --limit 500
