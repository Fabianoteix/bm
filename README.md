# BAMAQ Capital: processamento assíncrono de transações

Serviço que recebe transações financeiras por HTTP, grava no **MySQL**, processa de forma assíncrona via **Kafka** (análise de risco num serviço externo) e avisa outros sistemas quando o status muda. Foi escrito em **Python 3.11+** com **Arquitetura Hexagonal (Ports & Adapters)**.

> **Resumo das decisões.** O MySQL é a fonte da verdade. O problema de gravar em dois lugares (*dual write*) é resolvido com um **Transactional Outbox**. A entrega é **at-least-once**, e os consumidores são **idempotentes** graças a um fencing de tentativa no domínio somado a lock otimista. Os retries de longa duração são **agendados no próprio outbox** (`available_at`), então não travam partições. **Não há Redis**: depois de avaliado, ele não pagava o próprio custo aqui (ver [5.1](#51-por-que-não-usamos-redis)).

![Arquitetura](docs/architecture.png)

*O desenho também está em [SVG](docs/architecture.svg). Ele é gerado por [`docs/diagram/build_architecture_svg.py`](docs/diagram/build_architecture_svg.py).*

---

## Sumário

1. [Como executar](#1-como-executar)
2. [API](#2-api)
3. [Arquitetura](#3-arquitetura)
4. [Ciclo de vida da transação](#4-ciclo-de-vida-da-transação)
5. [Decisões e trade-offs](#5-decisões-e-trade-offs)
6. [Cenários de falha](#6-cenários-de-falha)
7. [Requisitos de confiabilidade: onde cada um é atendido](#7-requisitos-de-confiabilidade--onde-cada-um-é-atendido)
8. [Observabilidade](#8-observabilidade)
9. [Estratégia de testes](#9-estratégia-de-testes)
10. [Evoluções feitas após a primeira entrega](#10-evoluções-feitas-após-a-primeira-entrega)
11. [Limitações conhecidas e próximos passos](#11-limitações-conhecidas-e-próximos-passos)

---

## 1. Como executar

### Ambiente completo (Docker Compose)

```bash
make up          # mysql, kafka (KRaft), migrations, api, relay x2, worker x3, risk-mock
make logs        # logs JSON de api/worker/relay
make e2e         # testes ponta a ponta contra o ambiente
make up-obs      # + Prometheus com alertas (localhost:9090), Grafana (localhost:3000) e Kafka UI (localhost:8080)
make alerts-test # valida e testa as regras de alerta (promtool)
make down
```

| Serviço | Porta | Função |
|---|---|---|
| `api` | 8000 | `POST/GET /transactions`, `/health/*`, `/metrics`, `/docs` (OpenAPI) |
| `risk-mock` | 8081 | Mock do serviço de risco com injeção de falhas |
| `relay` | 9000 (métricas) | Publica o outbox no Kafka |
| `worker` | 9000 (métricas) | Consome e processa as transações |
| `kafka` | 29092 (host) | Broker |
| `mysql` | 3306 | Persistência (fonte da verdade) |

### Só os testes (sem Docker)

```bash
python -m venv .venv && source .venv/bin/activate
make install
make lint typecheck test         # ruff + mypy --strict + pytest
make cov                         # com cobertura

# a mesma suíte de integração contra um MySQL real:
TEST_DATABASE_URL=mysql+pymysql://app:app@localhost:3306/transactions_test pytest tests/integration tests/api
```

### Simulando falhas

```bash
# Cenário 3: serviço de risco fora por 30 minutos
curl -X POST localhost:8081/admin/outage -d '{"seconds": 1800}' -H 'content-type: application/json'
curl -X DELETE localhost:8081/admin/outage                          # volta antes

# taxa de erro / lentidão
curl -X POST localhost:8081/admin/chaos -H 'content-type: application/json' \
     -d '{"failure_rate": 0.5, "slow_rate": 0.2, "slow_seconds": 5}'

# regras determinísticas do mock
#   value > 10000            -> REJECTED
#   customer_id "blocked*"   -> REJECTED
#   customer_id "always-fail"-> 503 sempre (esgota tentativas -> FAILED + DLQ)
#   customer_id "bad-request"-> 400 (erro permanente -> FAILED imediato)

# reprocessar o que falhou
docker compose run --rm api transactions-admin reprocess-failed --limit 500
docker compose run --rm api transactions-admin reprocess <uuid> [<uuid> ...]
```

---

## 2. API

### `POST /transactions`

```bash
curl -i -X POST localhost:8000/transactions \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: pedido-42' \
  -d '{"customer_id": "123", "value": 1500.00}'
```

```http
HTTP/1.1 202 Accepted
Location: /transactions/550e8400-e29b-41d4-a716-446655440000
X-Correlation-ID: 6f1c...

{"id":"550e8400-...","customer_id":"123","value":1500.0,"status":"PENDING","attempts":0, ...}
```

* **202 Accepted**: a transação foi aceita e gravada, mas o processamento ainda vai acontecer. O cliente acompanha pelo `Location`.
* **`Idempotency-Key`** (opcional): repetir a chamada com a mesma chave e o mesmo payload devolve **200** e a mesma transação, com o header `Idempotent-Replayed: true`. Com payload diferente, a resposta é **409**.
* **422**: payload inválido (`value <= 0`, mais de 2 casas decimais, campo desconhecido, `customer_id` vazio etc.).
* Os erros seguem o **RFC 9457** (`application/problem+json`) e sempre trazem o `correlation_id`.

### `GET /transactions/{id}`

```json
{"id":"550e8400-...","customer_id":"123","value":1500.0,"status":"APPROVED",
 "attempts":1,"last_error":null,"created_at":"...","updated_at":"..."}
```

Um id inexistente devolve **404** (problem+json). Um id que não é UUID devolve **422**.

---

## 3. Arquitetura

### Hexagonal: a regra de dependência

```
src/transactions/
├── domain/                 # entidade Transaction, máquina de estados, eventos. Zero dependências de infra.
├── application/            # casos de uso + PORTAS (Protocols) + RetryPolicy
│   ├── ports.py            #   UnitOfWork, TransactionRepository, Outbox, RiskAnalysisGateway, Clock
│   └── use_cases/          #   CreateTransaction, GetTransaction, ProcessTransaction, ReprocessTransaction
├── adapters/               # detalhes externos, todos substituíveis
│   ├── http/               #   FastAPI (entrada)
│   ├── messaging/          #   consumer Kafka (entrada), producer, outbox relay, envelope/versionamento
│   ├── persistence/        #   SQLAlchemy/MySQL: repositório, outbox, UnitOfWork
│   └── risk/               #   cliente HTTP do serviço de risco + circuit breaker
├── observability/          # logs estruturados, métricas Prometheus
├── bootstrap.py            # composition root: único lugar que conhece os adapters concretos
└── entrypoints/            # api, worker, relay, admin (CLI)
```

`domain` não importa nada de fora. `application` depende só de `domain` e das próprias portas. Kafka, MySQL, o serviço de risco e o FastAPI são **adapters**. Na prática isso significa, por exemplo:

* `ProcessTransaction` não sabe que existe Kafka. Ele recebe `(transaction_id, attempt)` e devolve um `ProcessingOutcome`. Quem traduz mensagem em chamada e resultado em commit de offset é o adapter do consumer.
* A classificação "erro transitório ou permanente" do serviço de risco fica no adapter HTTP. A política de retry *entre tentativas* é regra de aplicação (`RetryPolicy`).
* O domínio emite `DomainEvent`s. O adapter decide o tópico e o formato do envelope, então o contrato externo pode ser versionado sem mexer na regra de negócio.

### Componentes em execução

| Processo | Escala | Por quê |
|---|---|---|
| **API** | N réplicas stateless | Só escreve no MySQL. **Não depende do Kafka** para aceitar transações. |
| **Outbox Relay** | N réplicas | Reserva as linhas com um *lease* (`SKIP LOCKED` numa transação curta), então as instâncias dividem o trabalho sem duplicar publicação e sem travar o banco enquanto esperam o Kafka. |
| **Worker** | até o nº de partições | Cada partição tem um único dono no consumer group. |
| **Admin CLI** | sob demanda | Reprocessamento e housekeeping do outbox. |

### Tópicos

| Tópico | Chave | Conteúdo | Quem consome |
|---|---|---|---|
| `transactions.processing.v1` | `transaction_id` | `ProcessingRequested{attempt}` | worker (fila de trabalho interna) |
| `transactions.events.v1` | `transaction_id` | `TransactionCreated`, `TransactionStatusChanged{version}` | **outros sistemas** |
| `transactions.processing.dlq.v1` | `transaction_id` | `TransactionDeadLettered` e mensagens *poison* (payload original + headers `dlq_*`) | operação / alertas |

Separar a **fila de trabalho interna** dos **eventos de integração** deixa o contrato público estável, mesmo quando o mecanismo interno de retry muda.

---

## 4. Ciclo de vida da transação

```mermaid
stateDiagram-v2
    [*] --> PENDING: POST /transactions
    PENDING --> APPROVED: risco APPROVED
    PENDING --> REJECTED: risco REJECTED
    PENDING --> RETRYING: falha transitória
    RETRYING --> RETRYING: nova falha transitória (backoff)
    RETRYING --> APPROVED
    RETRYING --> REJECTED
    PENDING --> FAILED: erro permanente
    RETRYING --> FAILED: tentativas esgotadas
    FAILED --> PENDING: reprocessamento manual
    APPROVED --> [*]
    REJECTED --> [*]
```

* `APPROVED` e `REJECTED` são **finais e imutáveis**: a máquina de estados rejeita qualquer transição a partir deles.
* `FAILED` é um estado "estacionado": a transação não será retentada automaticamente, mas pode ser reprocessada por operação.
* **Não existe um estado `PROCESSING` persistido.** Ele custaria uma escrita extra por mensagem e criaria o problema de "PROCESSING órfão" quando o worker cai. O fencing por tentativa (seção 5) cobre a necessidade real, que é evitar trabalho duplicado.

### Fluxo feliz e retry

```mermaid
sequenceDiagram
    autonumber
    participant C as Cliente
    participant A as API
    participant DB as MySQL
    participant R as Outbox Relay
    participant K as Kafka
    participant W as Worker
    participant RS as Serviço de risco

    C->>A: POST /transactions
    A->>DB: 1 COMMIT: INSERT transactions (PENDING) + INSERT outbox (Created, ProcessingRequested tentativa 1)
    A-->>C: 202 + Location
    R->>DB: COMMIT curto: SKIP LOCKED + locked_until = now + 45s (lease)
    R->>K: produce (acks=all, key=tx_id), sem transação aberta
    R->>DB: COMMIT curto: UPDATE outbox SET published_at
    K->>W: ProcessingRequested (tentativa 1)
    W->>DB: SELECT transaction (sem lock)
    W->>RS: POST /risk-analysis (Idempotency-Key)
    alt resposta APPROVED/REJECTED
        W->>DB: 1 COMMIT: UPDATE … WHERE version=v + INSERT outbox (StatusChanged)
    else 5xx / timeout / circuit aberto
        W->>DB: 1 COMMIT: UPDATE status=RETRYING + INSERT outbox (tentativa 2, available_at=now+backoff)
    end
    W->>K: store offset (commit periódico)
```

---

## 5. Decisões e trade-offs

| # | Decisão | Alternativas consideradas | Por que esta / o que custa |
|---|---|---|---|
| D1 | **Transactional Outbox** com relay por polling e **lease** (`locked_until`) | Publicar direto no Kafka após o commit; relay segurando `FOR UPDATE` durante o flush; CDC (Debezium) | Elimina o dual write sem infra extra. O lease faz a reserva e a confirmação em transações de milissegundos, então um Kafka lento não segura lock nem conexão no MySQL. **Custo:** latência de polling (~200 ms) e carga de leitura no MySQL. O Debezium tiraria o polling, mas **publica no INSERT e ignora `available_at`**, o que quebraria o retry agendado (D5); só compensa com volume muito maior e com o retry migrado para tópicos com delay. |
| D2 | **At-least-once + consumidor idempotente** | Exactly-once do Kafka (transações) | O EOS do Kafka não cobre o efeito no MySQL nem a chamada HTTP externa. A idempotência precisa existir no consumidor de qualquer jeito. |
| D3 | **Fencing de tentativa no domínio** (`attempt == attempts + 1` e status processável) | Tabela `processed_messages` (inbox) | Deduplica sem uma escrita extra por mensagem e também descarta mensagens **obsoletas** (retry antigo), não só as idênticas. Um inbox genérico continuaria sendo a escolha se houvesse muitos tipos de mensagem sem máquina de estados. |
| D4 | **Lock otimista** (`UPDATE … WHERE version = ?`) | `SELECT … FOR UPDATE` durante o processamento | Nenhum lock fica aberto durante a chamada HTTP (que pode levar segundos). Em rebalance, dois workers podem processar ao mesmo tempo: só um grava e o outro vira `CONCURRENT_UPDATE`, sem efeito. **Custo:** o serviço de risco pode ser chamado duas vezes, o que é mitigado pelo `Idempotency-Key = transaction_id`. |
| D5 | **Retry agendado via `outbox.available_at`** | Tópicos de retry com delay (`retry-5s`, `retry-1m`…); `sleep` no consumer | Não bloqueia a partição (as outras transações continuam fluindo), o agendamento é **atômico** com a mudança de status, sobrevive a crash e aparece numa query SQL. **Custo:** o tráfego de retry passa pelo MySQL. Tópicos de retry são a evolução natural se isso virar gargalo. |
| D6 | **Duas camadas de retry**: 2 retries curtos (ms) no adapter + retries agendados (s→min) | Só uma camada | Soluços de rede se resolvem em milissegundos, sem reenfileirar. Indisponibilidades longas saem do caminho quente. |
| D7 | **Circuit breaker por processo, com o estado de cada um exportado** | Breaker compartilhado (Redis ou tabela no MySQL) | É simples e sem dependência nova. Com N workers, o serviço recebe até N sondas por janela, e com o backpressure cada worker pausa sozinho. O ponto cego (falha parcial escondida na taxa de erro agregada) é coberto por observabilidade: gauge de estado por worker, alerta por instância e por fração da frota (seção 8). Compartilhar o estado só se a métrica mostrar necessidade. |
| D8 | **Sem Redis**: o `GET` lê direto do MySQL por chave primária | Redis como cache de leitura (cache-aside); Redis para idempotência, locks, rate limit ou breaker | Detalhado em [5.1](#51-por-que-não-usamos-redis). Em resumo: o cache quase não acertaria, a leitura por PK já é barata, e toda garantia de corretude precisa estar no mesmo COMMIT do MySQL. **Custo:** todo polling chega ao banco; a mitigação escala por réplica de leitura antes de cache. |
| D9 | **Chave = `transaction_id`** | Chave por cliente | Garante ordem por transação, que é a ordem que importa. Ordenação por cliente reduziria o paralelismo sem necessidade de negócio. |
| D10 | **Envelope versionado** (`event_id`, `event_type`, `schema_version`) + *upcasters* + tolerant reader; sufixo `.v1` no tópico | Schema Registry (Avro/Protobuf) | Resolve a evolução sem infraestrutura extra no desafio. Mudança aditiva incrementa `schema_version`. Mudança incompatível cria um tópico `.v2` com publicação dupla durante a migração. Em produção, o Schema Registry com compatibilidade `BACKWARD` seria a recomendação. |
| D11 | `DECIMAL(14,2)` + `Decimal` no domínio | float | Dinheiro não pode sofrer erro de ponto flutuante. Na API o valor sai como número JSON, conforme o enunciado. |
| D12 | UUID como `CHAR(36)` | `BINARY(16)` / UUIDv7 | Legibilidade para debug. **Custo:** índice maior e inserções aleatórias na PK. Com alto volume, usar **UUIDv7 em `BINARY(16)`** (ordenável no tempo). |

### 5.1 Por que não usamos Redis

O enunciado pede avaliar um uso justificado de Redis. A primeira versão o usava como cache do `GET /transactions/{id}`. Reavaliando, cada uso possível foi descartado:

| Uso possível | Por que não |
|---|---|
| **Cache do `GET`** | **O cache quase não acertaria.** O cliente consulta repetidamente *enquanto* o status está `PENDING`/`RETRYING` e para quando ele fica final. Então as leituras repetidas caem justamente no dado que muda, que exigia TTL de 2 s e invalidação a cada commit; o status final, que seria seguro cachear, quase não é relido. Além disso, a leitura é por **chave primária** (~1 ms). Mesmo no Cenário 5 (10 mil transações/min, ~5 consultas cada) são ~830 leituras/s simples, o que um MySQL atende, e réplica de leitura escala isso sem mudar a consistência. |
| **Idempotência e locks** | A garantia precisa estar **no mesmo COMMIT** que grava a transação. `UNIQUE(customer_id, idempotency_key)` e `version` no MySQL fazem isso de forma atômica. No Redis, a chave e a transação poderiam divergir numa falha entre as duas escritas: seria o mesmo *dual write* que o outbox resolve. |
| **Rate limit** | Fora do escopo do desafio e, em produção, responsabilidade do API gateway, não do serviço. |
| **Circuit breaker compartilhado** | O backpressure já pausa cada worker quando o seu circuito abre. O ganho de sincronizar N workers não justifica mais uma dependência no caminho do processamento. |

**O que se ganha sem ele:**

* **Consistência forte na leitura:** o `GET` sempre mostra o último status gravado; não existe janela de dado velho nem invalidação que possa falhar em silêncio.
* **Menos modos de falha:** um componente a menos para subir, monitorar, dimensionar, proteger e testar (Redis lento, fora, sem memória).
* **Menos código:** saíram o adapter, a porta `TransactionCache`, as chamadas de invalidação nos casos de uso, a configuração e os testes específicos.

**Quando reintroduzir (gatilho objetivo, não palpite):** se o p99 do `GET` passar do SLO ou a carga de leitura pressionar o MySQL **mesmo com réplica de leitura**. Pela arquitetura hexagonal, isso seria uma porta nova e um adapter ligado no `bootstrap.py`, sem mudar domínio nem os outros casos de uso.

---

## 6. Cenários de falha

### Cenário 1: o MySQL persiste, mas a publicação no Kafka falha

**Isso não pode acontecer, por construção.** A API não publica no Kafka. O evento é gravado na tabela `outbox` **no mesmo COMMIT** da transação: ou os dois existem, ou nenhum existe. Se o Kafka estiver fora, o relay recebe erro no *delivery report*, a linha continua com `published_at = NULL`, `publish_attempts` é incrementado e o relay tenta de novo com backoff. Nada se perde e a API **continua aceitando transações** (o `/health/ready` não depende do Kafka). Se o relay cair entre a reserva e o `UPDATE published_at`, o lease de 45 s vence e outro relay publica o evento de novo; todo consumidor é idempotente. Com o Kafka **lento** (e não fora), só o relay espera: nenhuma transação ou lock de linha fica aberto no MySQL.
*Sinal operacional:* `outbox_oldest_pending_age_seconds` crescendo.
*Testes:* `test_kafka_down_keeps_events_for_later`, `test_persists_transaction_and_outbox_atomically`, `test_crashed_relay_lease_expires_and_another_relay_takes_over`, `test_slow_kafka_does_not_hold_row_locks`.

### Cenário 2: o consumer atualiza o banco e morre antes de confirmar o offset

O consumer usa `enable.auto.offset.store=false`: o offset só é marcado **depois** do COMMIT no MySQL. Se o processo morrer entre um e outro, a mensagem é reentregue a outro worker. Aí o fencing entra em ação: a transação já está `APPROVED` (ou `attempts` já avançou), então `accepts_attempt()` retorna falso, o caso de uso devolve `DUPLICATE`, o offset avança e **não há nova chamada ao serviço externo nem evento duplicado**. Se o crash foi *antes* do COMMIT, nada foi gravado e a reentrega processa normalmente.
*Testes:* `test_duplicate_message_is_ignored`, `test_redelivery_after_crash_before_commit_is_processed`, `test_offset_is_stored_only_after_successful_handling`.

### Cenário 3: o serviço externo fica indisponível por 30 minutos

1. **Timeouts curtos** (connect 1s / read 3s) impedem que um worker fique preso.
2. **2 retries rápidos** (200 ms, 400 ms, com jitter) absorvem soluços.
3. **Circuit breaker**: após 5 falhas consecutivas ele abre e as chamadas falham na hora, sem esperar timeout. A cada 30s passa uma única chamada de teste (*half-open*).
4. **Retry agendado**: a transação vai para `RETRYING` e um novo `ProcessingRequested#n+1` é gravado no outbox com `available_at = agora + backoff` (5s, 10s, 20s … até 10 min, ±20% de jitter). Com o padrão de **12 tentativas, a janela total é de ~50 min**, o que cobre os 30 min com folga. O worker **libera a partição** na hora, então as outras transações continuam fluindo.
5. Quando o serviço volta, o jitter espalha as tentativas e evita o *thundering herd*.
6. Se o serviço ficar fora além da janela, a transação vai para `FAILED`, um `TransactionDeadLettered` é emitido na DLQ, e `transactions-admin reprocess-failed` recoloca tudo no fluxo depois.

*Testes:* `test_transient_then_success`, `test_definitive_failure_after_max_attempts`, `test_default_budget_survives_30_minute_outage`, `test_open_circuit_fails_fast_without_calling_service`, e2e `test_outage_then_recovery`.

### Cenário 4: a mesma mensagem é entregue mais de uma vez

A idempotência está em várias camadas:

| Camada | Mecanismo |
|---|---|
| Entrada HTTP | `Idempotency-Key` com constraint `UNIQUE`. A corrida entre duas requisições é resolvida pelo banco. |
| Consumer | Fencing de tentativa + status final imutável. Mensagens duplicadas **e** obsoletas são descartadas. |
| Concorrência (rebalance) | `UPDATE … WHERE version = ?`: só um worker grava. |
| Serviço externo | `Idempotency-Key: <transaction_id>` enviado ao provedor. |
| Consumidores externos | `event_id` para deduplicar e `version` monotônico por transação para descartar evento fora de ordem. |

*Testes:* `test_duplicate_message_is_ignored`, `test_stale_retry_message_is_ignored`, `test_concurrent_consumers_only_one_wins`, `test_unique_constraint_resolves_concurrent_requests`, `test_idempotency_key`.

### Cenário 5: de 100 para 10.000 eventos/min

10.000/min dá **~167 eventos/s**. Para o MySQL e o Kafka isso é pouco. O gargalo real é a **latência do serviço de risco**.

* **Dimensionamento das partições:** com um worker sequencial e latência de ~100 ms, cada partição processa ~10 msg/s. Então `partições ≥ 167 × 0,1 × 2 (folga)` ≈ **34 → 48 partições, provisionadas desde o dia 1** (o compose já cria assim). **Nunca aumentar partições de um tópico em uso:** muda `hash(chave) mod N`, e eventos da mesma transação passam a cair em partições diferentes durante a transição. Escalar = subir workers (até 48). Se um dia precisar de mais partições, cria-se um tópico novo (`.v2`) e os produtores migram. No KRaft, partição ociosa custa pouco.
* **API / relay / worker** são stateless e escalam horizontalmente (HPA por CPU na API; por *consumer lag* nos workers, com KEDA, por exemplo).
* **Relay:** várias instâncias em paralelo (lease + `SKIP LOCKED`), lotes de até 500 eventos, `linger.ms` e compressão `lz4`. O índice `(published_at, available_at, id)` atende a consulta do relay, e `purge-outbox` impede o crescimento da tabela.
* **MySQL:** pool de conexões dimensionado, `READ COMMITTED`, escritas curtas (nenhuma transação de banco fica aberta durante IO externo).
* **Serviço de risco:** pool HTTP com keep-alive. Se ele não aguentar o novo volume, o circuit breaker e o backoff protegem os dois lados e o outbox absorve o pico (**backpressure** natural). O próximo passo seria rate limit/bulkhead por dependência.
* **Evolução:** concorrência dentro do worker (processar em paralelo chaves diferentes da mesma partição, mantendo a ordem por chave) para usar melhor cada partição.

---

## 7. Requisitos de confiabilidade: onde cada um é atendido

| Requisito | Onde |
|---|---|
| Processamento idempotente | `Transaction.accepts_attempt`, `SqlAlchemyTransactionRepository.update` (version) |
| Mensagens duplicadas | fencing + `ProcessingOutcome.DUPLICATE` |
| Estratégia de retry | `HttpRiskAnalysisClient` (curto) + `RetryPolicy` / `Transaction.schedule_retry` (agendado) |
| Limite de tentativas excedido | `Transaction.fail` → `FAILED` + `TransactionDeadLettered` na DLQ |
| Recuperação de falhas temporárias | circuit breaker, backoff com jitter, `seek` + backoff no consumer para falhas de infraestrutura (ex.: MySQL fora) |
| Consistência persistência × publicação | Transactional Outbox (`SqlAlchemyOutbox` + `OutboxRelay`) |
| Tratamento de erros | exceções tipadas por camada, problem+json na API, classificação transitório/permanente, poison → DLQ |
| Reprocessamento | `ReprocessTransaction` + CLI `transactions-admin` (parte do MySQL, não dos bytes da DLQ) |
| Ordenação | chave `transaction_id`, producer idempotente, `version` nos eventos de status |
| Versionamento de eventos | envelope com `schema_version`, `UPCASTERS`, tolerant reader, sufixo `.vN` no tópico |

---

## 8. Observabilidade

* **Logs estruturados (JSON, structlog)** com `correlation_id`, `transaction_id`, `event_id`, `attempt`, `kafka_partition`/`kafka_offset`, `service`, `env`.
  * O `correlation_id` vem do header `X-Correlation-ID` (ou é gerado), é gravado nos headers do outbox, vai para os headers do Kafka, é religado no worker e **enviado ao serviço de risco**. Com isso dá para seguir uma transação da API até a DLQ.
  * Eventos relevantes: `transaction.created`, `processing.transient_failure` (com `retry_in_seconds`), `processing.max_attempts_exceeded`, `processing.permanent_failure`, `processing.duplicate_skipped`, `consumer.sent_to_dlq`, `outbox.publish_failed`, `outbox.lease_reclaimed`, `circuit.state_changed` (com estado anterior, novo e host).
* **Métricas Prometheus** (`/metrics` na API; porta 9000 no worker e no relay):

| Métrica | Para quê |
|---|---|
| `http_requests_total`, `http_request_duration_seconds` | RED da API |
| `transactions_processing_outcomes_total{outcome}` | taxa de aprovação/rejeição/retry/falha/duplicata |
| `risk_service_request_duration_seconds{outcome}` | latência e erros da dependência externa |
| `risk_service_circuit_state` (0 fechado, 1 meio aberto, 2 aberto), por worker | **falha parcial**: quais workers pararam (`risk_service_circuit_open` mantida por compatibilidade) |
| `risk_service_circuit_transitions_total{to_state}` | circuito oscilando |
| `transactions_consumer_paused` | worker em backpressure |
| `outbox_pending_events`, `outbox_oldest_pending_age_seconds` | **lag do outbox** (Cenário 1) |
| `outbox_lease_reclaimed_total` | relay que morreu ou travou no meio de um lote |
| `transactions_poison_messages_total`, `transactions_consumer_errors_total` | DLQ e falhas de infraestrutura |

* **Por que o estado de cada worker:** o circuit breaker é local. Se 3 de 20 workers abrirem o circuito, a taxa de erro agregada quase não muda e um alerta sobre ela fica mudo, enquanto as partições desses 3 acumulam lag. O Prometheus coleta cada worker como um alvo próprio (label `instance`), então o estado de cada um aparece sem estado compartilhado.
* **Alertas versionados** em [`ops/alerts.yml`](ops/alerts.yml), com testes do `promtool` em [`ops/alerts_test.yml`](ops/alerts_test.yml) rodando na CI:

| Alerta | Condição | Severidade |
|---|---|---|
| `RiskCircuitOpenOnWorker` | um worker com o circuito aberto há 3 min | warning |
| `RiskCircuitOpenFleetWide` | 50% ou mais dos workers abertos há 2 min | critical |
| `RiskCircuitFlapping` | mais de 3 aberturas em 15 min no mesmo worker | warning |
| `OutboxLagHigh` | evento mais antigo do outbox esperando mais de 60 s há 2 min | critical |

* **Painel Grafana** provisionado ([`ops/grafana/`](ops/grafana/)): linha do tempo do estado do breaker com uma faixa por worker, workers abertos e fração da frota, aberturas por worker, lag do outbox e resultados do processamento.
* **Próximos alertas:** qualquer mensagem na DLQ, consumer lag crescente, taxa de `FAILED` acima de X%.
* **Health checks:** `/health/live` (processo vivo) e `/health/ready` (MySQL acessível; o Kafka fica de fora de propósito).
* **Próximo passo:** OpenTelemetry (trace `traceparent` no mesmo caminho do `correlation_id`).

---

## 9. Estratégia de testes

Pirâmide com foco em **comportamento**, usando dublês só para o que é externo e não-determinístico:

| Nível | O que cobre | Infra |
|---|---|---|
| **Unit** (`tests/unit`) | máquina de estados, fencing, validações, `RetryPolicy`, circuit breaker, classificação de erros do cliente HTTP (via `httpx.MockTransport`), serialização/upcasting/poison | nenhuma |
| **Integração** (`tests/integration`) | casos de uso com **persistência real (SQLAlchemy)**: outbox atômico, duplicidade, retry, falha definitiva, concorrência (lock otimista), corrida de idempotência, relay (Kafka fora, retry atrasado, purge, **SKIP LOCKED com 2 relays em paralelo**, **lease**: reserva commitada antes de publicar, relay que morre e lease que vence, falha que não libera lease alheio, **nenhum lock durante Kafka lento** com `NOWAIT` no MySQL), telemetria do circuit breaker (3 estados, log por transição), handler do consumer (poison, órfã, DLQ indisponível, erro de infra), loop do consumer (offsets/seek), `GET` refletindo o último status gravado | SQLite em memória por padrão; **MySQL real** com `TEST_DATABASE_URL` |
| **API** (`tests/api`) | contrato HTTP: 202/200/404/409/422/500, problem+json, correlation id, health, métricas | igual à integração |
| **E2E** (`tests/e2e`) | fluxo completo com Kafka, relay, worker e mock: aprovado, rejeitado, erro permanente, indisponibilidade e recuperação, 404 | `docker compose` (`make e2e`) |

Casos pedidos no enunciado:

| Caso | Teste |
|---|---|
| Fluxo de sucesso | `test_success_flow_approved`, `test_end_to_end_success_and_duplicate`, e2e `test_approved_flow` |
| Processamento duplicado | `test_duplicate_message_is_ignored`, `test_stale_retry_message_is_ignored` |
| Falha temporária do serviço externo | `test_transient_failure_schedules_delayed_retry`, `test_transient_then_success` |
| Falha definitiva após múltiplas tentativas | `test_definitive_failure_after_max_attempts` |
| Consulta de transação inexistente | `test_get_unknown_transaction_returns_404_problem`, `TestGet.test_not_found` |

Tempo e aleatoriedade são injetados (`Clock`, `sleep`, `rng`), então os testes são determinísticos e rodam em cerca de 2s. A CI (`.github/workflows/ci.yml`) roda lint, mypy `--strict`, testes com cobertura e o e2e contra o compose.

---

## 10. Evoluções feitas após a primeira entrega

| Evolução | Problema que resolve | Onde | Teste |
|---|---|---|---|
| **Backpressure no consumer** | Com o circuito do risco aberto, cada mensagem virava um retry agendado (UPDATE + INSERT no MySQL) sem nenhuma chance de sucesso. Agora as partições são pausadas e as mensagens esperam no Kafka; o consumo volta no HALF_OPEN, e a próxima mensagem é a sonda. | `KafkaProcessingConsumer._apply_backpressure`, `entrypoints/worker.py` | `test_pauses_while_circuit_open_and_resumes_after` |
| **Remoção do Redis** | O cache do `GET` tinha acerto baixo (o polling acontece enquanto o status muda), exigia invalidação a cada commit e criava janela de dado velho, em troca de poupar leituras por PK que já são baratas. Justificativa completa em [5.1](#51-por-que-não-usamos-redis). | `GetTransaction` (lê do MySQL), `bootstrap.py`, `docker-compose.yml` | `test_get_always_reflects_latest_status` |
| **Estado do circuit breaker por worker** | Com o breaker local, uma falha parcial (alguns workers com o circuito aberto) ficava escondida na taxa de erro agregada. Agora cada worker exporta seu estado (0/1/2), cada transição gera log, e há alertas por instância e por fração da frota, com painel Grafana. | `observability/circuit_breaker.py`, `ops/alerts.yml`, `ops/grafana/` | `test_circuit_telemetry.py`, `ops/alerts_test.yml` |
| **48 partições desde o dia 1** | O compose criava 12 partições, contra os 48 que o próprio Cenário 5 recomenda. Aumentar partições com o sistema rodando muda o mapeamento chave→partição e bagunça a ordem. | `docker-compose.yml` (`kafka-init`) | — |
| **Lease no outbox relay** | O relay segurava `FOR UPDATE` e a transação abertos durante o flush do Kafka (até 10 s com o Kafka lento). Agora reserva e confirmação são transações de milissegundos e a publicação acontece sem nada aberto no banco. | `OutboxRelay`, migration `0003` | `test_outbox_lease.py` |
| **Idempotency-Key por cliente** | Antes a chave era única globalmente: dois clientes que gerassem "pedido-1" colidiam (o segundo tomava 409). Agora é `UNIQUE(customer_id, idempotency_key)`. | migration `0002`, `models.py`, porta `get_by_idempotency_key(customer_id, key)` | `TestIdempotencyKeyPerCustomer` |

A migration `0002` cria a constraint nova antes de remover a antiga (sem janela desprotegida). A `0003` só adiciona colunas anuláveis, então o relay antigo convive com ela durante o deploy. As duas têm `downgrade` e foram validadas em MySQL 8: upgrade, `alembic check`, downgrade e upgrade.

As três últimas evoluções vieram de uma revisão externa da arquitetura. Uma quarta sugestão, trocar o relay por Debezium, foi avaliada e **não** adotada (ver D1).

## 11. Limitações conhecidas e próximos passos

* **Chamada externa duplicada em corrida de rebalance.** O lock otimista impede o efeito duplicado no banco, mas o serviço de risco pode ser chamado duas vezes. A mitigação é o `Idempotency-Key`; a garantia completa depende do provedor honrar essa chave.
* **Ordem dos eventos de integração entre relays paralelos.** Com vários relays, dois eventos da mesma transação podem sair fora de ordem em casos raros (por exemplo, `Created` falha e `StatusChanged` sai antes). Na prática a cadeia causal (o status só muda depois que o processamento foi publicado) torna isso quase impossível, e o campo `version` permite que o consumidor descarte o que chegar fora de ordem. Para ordem estrita, dá para usar um relay por partição lógica ou CDC.
* **O lease não impede publicação dupla:** se o relay travar além de 45 s (sem morrer), outro relay assume e os dois publicam. Continua sendo at-least-once, absorvido pelos consumidores idempotentes, e `outbox_lease_reclaimed_total` mostra quando acontece.
* **O circuit breaker é local a cada worker** (ver D7). Cada worker pausa sozinho ao abrir o seu circuito; um estado compartilhado faria todos pausarem juntos. A falha parcial agora é **visível** (seção 8), mas não é **coordenada**.
* **Todo `GET` chega ao MySQL** (ver 5.1). Com carga de leitura alta, o primeiro passo é uma réplica de leitura; cache só com métrica mostrando necessidade.
* **O outbox cresce:** `purge-outbox` precisa virar um job agendado (CronJob), ou a tabela deve ser particionada por data.
* **Sem autenticação/autorização na API** (fora do escopo). Em produção: OAuth2/mTLS, rate limit por cliente e PII mascarada nos logs.
* **Retry com a transação `RETRYING` não "anda" se o relay estiver parado.** Isso é intencional (o relay é o único publicador), e o alerta de lag do outbox cobre o caso.
* **Evoluções:** Debezium no lugar do relay por polling, Schema Registry, OpenTelemetry, concorrência por chave dentro do worker, UUIDv7 em `BINARY(16)`.
