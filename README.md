<div align="center">

# Financial Integration Layer

<p align="center">
  <strong>End-to-end пайплайн загрузки финансовых данных в аналитическое хранилище Data Vault 2.0</strong><br>
  Alpha Vantage + ЦБ РФ → Kafka → PostgreSQL → Spark → витрины → Metabase
</p>

<p align="center">
  <a href="https://github.com/ujine-kim/financial-integration-layer/actions/workflows/ci.yml">
    <img src="https://github.com/ujine-kim/financial-integration-layer/actions/workflows/ci.yml/badge.svg" alt="CI">
  </a>
  <img src="https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/Apache%20Kafka-3.7-231F20?logo=apachekafka&logoColor=white" alt="Kafka">
  <img src="https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark&logoColor=white" alt="Spark">
  <img src="https://img.shields.io/badge/Apache%20Airflow-2.9-017CEE?logo=apacheairflow&logoColor=white" alt="Airflow">
  <img src="https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white" alt="PostgreSQL">
  <img src="https://img.shields.io/badge/Pydantic-v2-E92063?logo=pydantic&logoColor=white" alt="Pydantic">
  <img src="https://img.shields.io/badge/tests-27%20passed-brightgreen?logo=pytest&logoColor=white" alt="Tests">
</p>

</div>

---

## О проекте

Пет-проект с production-паттернами: ежедневная загрузка биржевых котировок и валютных курсов из публичных API, буферизация через Kafka, трансформация Spark'ом и укладка в Data Vault 2.0. Весь пайплайн оркестрируется Airflow и разворачивается одной командой через Docker Compose.

**Что реализовано:**
- Streaming ingestion через Kafka KRaft (без Zookeeper)
- Data Vault 2.0 в PostgreSQL: Hub / Link / Satellite + delta-load по `hash_diff`
- Идемпотентные Spark-джобы (повторный запуск не создаёт дублей)
- Retry с exponential backoff для нестабильных API
- Pydantic v2 для валидации входящих данных на границе системы
- Версионированные DDL-миграции через Liquibase
- CI/CD: lint + test + docker validate на каждый push

---

## Архитектура

```
╔══════════════════════════════════════════════════════════════╗
║                      ИСТОЧНИКИ ДАННЫХ                        ║
╠══════════════════╦═══════════════════════════════════════════╣
║  Alpha Vantage   ║   Котировки AAPL, MSFT, GOOGL...         ║
║  ЦБ РФ XML API   ║   54 валюты к рублю (без API-ключа)      ║
╚════════╤═════════╩═══════════════════════════════════════════╝
         │ HTTP → Pydantic v2 валидация → Kafka Producer
         ▼
╔══════════════════════════════════════════════════════════════╗
║                    Apache Kafka 3.7 (KRaft)                  ║
║                                                              ║
║   quotes.raw  [p0│p1│p2]   ключ: AAPL:2025-06-06           ║
║   cbr.rates.raw [p0│p1│p2] ключ: USD:2025-06-06            ║
╚════════╤═════════════════════════════════════════════════════╝
         │ Kafka Consumer → batch insert
         ▼
╔══════════════════════════════════════════════════════════════╗
║                   PostgreSQL 16  (raw схема)                 ║
║   raw.quotes          raw.rates                              ║
╚════════╤═════════════════════════════════════════════════════╝
         │ PySpark — delta-load по hash_diff
         ▼
╔══════════════════════════════════════════════════════════════╗
║               Data Vault 2.0  (vault схема)                  ║
║                                                              ║
║  Hubs (бизнес-ключи)    Links (связи)   Satellites (история)║
║  hub_instrument ────────── lnk_quote ── sat_quote_prices    ║
║  hub_currency   ──────── lnk_rate ───── sat_rate_values     ║
║  hub_market                                                  ║
╚════════╤═════════════════════════════════════════════════════╝
         │ SQL витрины
         ▼
╔══════════════════════════════════════════════════════════════╗
║                  Data Marts  (dm схема)                      ║
║   dm_quotes_daily  — дневная доходность, диапазон цен        ║
║   dm_fx_rates      — кросс-курсы валют                       ║
╚════════╤═════════════════════════════════════════════════════╝
         │
         ▼
╔══════════════════════════════════════════════════════════════╗
║                        Metabase                              ║
║         дашборды · графики · ad-hoc SQL запросы             ║
╚══════════════════════════════════════════════════════════════╝

                    ↕ оркестрация (Apache Airflow 2.9)
         ┌──────────────────────────────────────────┐
         │  18:00 пн-пт │ ingest_quotes_daily        │
         │  триггер     │ spark_raw_to_vault          │
         │  20:00 пн-пт │ build_dm_marts             │
         └──────────────────────────────────────────┘

                    ↕ файловое хранилище
         ┌──────────────────────────────────────────┐
         │  MinIO (S3-совместимый)                   │
         │  raw-zone / vault-zone / dm-zone          │
         └──────────────────────────────────────────┘
```

---

## Технологический стек

| Слой | Технология | Версия | Зачем |
|---|---|---|---|
| Брокер сообщений | Apache Kafka KRaft | 3.7 | Буферизация, декаплинг producer/consumer, replay |
| Batch обработка | Apache Spark (PySpark) | 3.5 | Масштабируемый ETL, встроенные функции быстрее UDF |
| Оркестрация | Apache Airflow | 2.9 | DAG-зависимости, retry, мониторинг |
| Хранилище | PostgreSQL | 16 | ACID, `ON CONFLICT DO NOTHING` для идемпотентности |
| Файловое хранилище | MinIO | latest | S3-совместимость для Spark |
| Визуализация | Metabase | latest | SQL-дашборды без кода |
| Валидация данных | Pydantic v2 | 2.7 | Типизация на границе системы, автоматическая конвертация типов |
| DDL-миграции | Liquibase | — | Версионирование схемы, rollback |
| Линтер | Ruff | 0.4 | Быстрее flake8 + black в одном инструменте |
| Тесты | pytest | 8.2 | Unit-тесты с фикстурами реальных API-ответов |

---

## Data Vault 2.0

Хранилище разделено на три типа таблиц. Каждый тип решает конкретную проблему:

```
HUB                          LINK                       SATELLITE
──────────────────           ──────────────────────     ─────────────────────────
hub_instrument               lnk_quote_instrument       sat_quote_prices
  hub_instrument_hk (PK) ◄──  hub_instrument_hk (FK)     hub_instrument_hk (FK)
  instrument_bk                hub_market_hk (FK)         load_date
  load_date                    load_date                  hash_diff  ← дельта-ключ
  record_source                record_source              open_price
                                                          high_price
hub_currency                 lnk_rate_currency            low_price
  hub_currency_hk (PK) ◄────  hub_currency_hk_base (FK)  close_price
  currency_code                hub_currency_hk_tgt (FK)   volume
  load_date                    load_date                  trade_date
  record_source                record_source
                                                        sat_rate_values
hub_market                                                lnk_rate_currency_hk (FK)
  hub_market_hk (PK)                                      load_date
  market_code                                             hash_diff
  load_date                                               rate
  record_source                                           rate_date
```

**Ключевые решения:**

| Принцип | Реализация |
|---|---|
| Append-only Hubs | `INSERT ... ON CONFLICT DO NOTHING` — бизнес-ключ попадает один раз |
| Delta-load | `hash_diff` = MD5 от атрибутов; новая строка Satellite только при изменении данных |
| Идемпотентность | Повторный запуск Spark-джоба не создаёт дублей — безопасный retry |
| Audit trail | `load_date` + `record_source` на каждой строке — полный lineage |
| NULL-safe хэш | `coalesce(col, 'NULL')` перед конкатенацией — `MD5(NULL) ≠ MD5('')` |

---

## Как движутся данные (шаг за шагом)

```
1. Airflow запускает DAG ingest_quotes_daily в 18:00

2. CBRProducer / AlphaVantageProducer:
   - HTTP GET к API с retry (tenacity: 3 попытки, exponential backoff)
   - Парсинг ответа → Pydantic v2 валидация (RawRate / RawQuote)
   - kafka.produce(key="USD:2025-06-06", value=json)

3. Kafka хранит сообщения в топике cbr.rates.raw (3 партиции)

4. Consumer читает батч → batch INSERT в raw.rates (PostgreSQL)

5. Airflow триггерит spark_raw_to_vault

6. Spark-джоб LoadHubInstrument:
   - Читает raw.quotes за дату
   - hash_key_column("symbol") → hub_instrument_hk
   - INSERT в vault.hub_instrument ON CONFLICT DO NOTHING

7. Spark-джоб LoadSatQuotePrices:
   - Читает raw.quotes + последний hash_diff из sat_quote_prices
   - generate_hash_diff(df, ["open_price", "close_price", ...])
   - Фильтрует только строки с изменившимся hash_diff
   - INSERT только дельту

8. В 20:00 DAG build_dm_marts обновляет витрины dm_quotes_daily, dm_fx_rates

9. Metabase рисует дашборды из dm схемы
```

---

## Быстрый старт

### Требования

- Docker Desktop 4.x
- Python 3.11+
- Java 17+ (для локального запуска Spark без Docker)
- Git Bash (на Windows)

### 1. Клонировать и настроить

```bash
git clone https://github.com/ujine-kim/financial-integration-layer.git
cd financial-integration-layer

cp .env.example .env
# Вписать ALPHA_VANTAGE_API_KEY в .env
# Бесплатный ключ: https://www.alphavantage.co/support/#api-key
# ЦБ РФ работает без ключа
```

### 2. Поднять инфраструктуру

```bash
# Поднять PostgreSQL + Kafka
docker compose up -d postgres kafka
docker compose up kafka-init        # создать топики (завершится сам)

# Поднять Airflow + Metabase
docker compose up -d airflow-webserver airflow-scheduler metabase
```

### 3. Применить DDL-миграции

```bash
docker exec -i fil-postgres psql -U dwh -d dwh < liquibase/changelog/v001_create_hubs.sql
docker exec -i fil-postgres psql -U dwh -d dwh < liquibase/changelog/v002_create_links.sql
docker exec -i fil-postgres psql -U dwh -d dwh < liquibase/changelog/v003_create_satellites.sql
```

### 4. Python-зависимости и тесты

```bash
pip install -r src/requirements.txt
python -m pytest tests/unit/ -v
# 27 passed
```

### 5. Запустить пайплайн вручную

```bash
# Загрузка в Kafka
python -m src.ingestion.producers.cbr_producer
python -m src.ingestion.producers.alpha_vantage_producer

# Kafka → PostgreSQL raw
python -m src.ingestion.consumers.rates_consumer
python -m src.ingestion.consumers.quotes_consumer

# Spark ETL: raw → Data Vault
python -m src.processing.raw_to_vault.load_hub_instrument --date $(date +%Y-%m-%d)
python -m src.processing.raw_to_vault.load_sat_quote_prices --date $(date +%Y-%m-%d)
```

### 6. Веб-интерфейсы

| Сервис | URL | Логин |
|---|---|---|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| Metabase | http://localhost:3000 | задаётся при первом входе |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin123` |

---

## Структура проекта

```
financial-integration-layer/
│
├── src/
│   ├── common/
│   │   ├── config.py              # Pydantic Settings — читает .env
│   │   └── vault_utils.py         # hash_key, hash_diff, vault_metadata (Spark API)
│   │
│   ├── ingestion/
│   │   ├── schemas/               # Pydantic v2 модели входящих данных
│   │   │   ├── quote.py           # RawQuote — котировка из Alpha Vantage
│   │   │   ├── rate.py            # RawRate  — валютный курс из ЦБ РФ
│   │   │   └── instrument.py      # RawInstrument — метаданные инструмента
│   │   ├── producers/
│   │   │   ├── base_producer.py   # BaseProducer (ABC) — context manager, flush
│   │   │   ├── cbr_producer.py    # XML → retry → нормализация номинала → Kafka
│   │   │   └── alpha_vantage_producer.py
│   │   └── consumers/
│   │       ├── quotes_consumer.py # Kafka → raw.quotes
│   │       └── rates_consumer.py  # Kafka → raw.rates
│   │
│   └── processing/
│       ├── base/spark_job.py      # SparkJob (ABC) — инициализация SparkSession
│       └── raw_to_vault/
│           ├── load_hub_instrument.py    # raw → hub_instrument (idempotent)
│           └── load_sat_quote_prices.py  # raw → sat_quote_prices (delta-load)
│
├── airflow/
│   ├── Dockerfile                 # кастомный образ: Java + PySpark + зависимости
│   └── dags/
│       ├── base/base_dag.py       # фабрика DAG'ов с общими параметрами
│       ├── dag_ingest_quotes.py   # 18:00 пн-пт
│       ├── dag_spark_raw_to_vault.py  # триггерный DAG
│       └── dag_build_marts.py     # 20:00 пн-пт
│
├── liquibase/
│   └── changelog/
│       ├── v001_create_hubs.sql       # hub_instrument, hub_currency, hub_market
│       ├── v002_create_links.sql      # lnk_quote_instrument, lnk_rate_currency
│       └── v003_create_satellites.sql # sat_quote_prices, sat_rate_values
│
├── sql/marts/
│   ├── dm_quotes_daily.sql        # дневная доходность и ценовой диапазон
│   └── dm_fx_rates.sql            # кросс-курсы валют
│
├── tests/
│   ├── fixtures/
│   │   ├── cbr_response.xml           # реальный XML-ответ ЦБ РФ
│   │   └── alpha_vantage_response.json
│   └── unit/ingestion/
│       ├── test_schemas.py            # валидация Pydantic моделей
│       ├── test_cbr_producer.py       # парсинг XML, нормализация номинала
│       └── test_alpha_vantage_producer.py
│
├── .github/workflows/ci.yml       # lint + test + docker validate (параллельно)
├── docker-compose.yml
├── pyproject.toml                 # ruff + pytest + mypy
└── .env.example
```

---

## Kafka топики

| Топик | Партиции | Источник | Формат ключа |
|---|---|---|---|
| `quotes.raw` | 3 | Alpha Vantage | `{SYMBOL}:{YYYY-MM-DD}` |
| `cbr.rates.raw` | 3 | ЦБ РФ | `{CCY}:{YYYY-MM-DD}` |
| `rates.raw` | 3 | ExchangeRate API | `{CCY}:{YYYY-MM-DD}` |
| `instruments.raw` | 3 | Alpha Vantage | `{SYMBOL}` |

Ключ сообщения обеспечивает попадание одного инструмента/валюты всегда в одну партицию — порядок сообщений гарантирован на уровне ключа.

---

## Переменные окружения

| Переменная | Описание | По умолчанию |
|---|---|---|
| `ALPHA_VANTAGE_API_KEY` | Ключ Alpha Vantage | — (обязательно) |
| `EXCHANGE_RATE_API_KEY` | Ключ ExchangeRate API | — (опционально) |
| `KAFKA_BOOTSTRAP_SERVERS` | Адрес Kafka брокера | `localhost:9092` |
| `MINIO_ENDPOINT` | URL MinIO | `http://localhost:9000` |
| `MINIO_ACCESS_KEY` | MinIO логин | `minioadmin` |
| `MINIO_SECRET_KEY` | MinIO пароль | `minioadmin123` |
| `DWH_HOST` | Хост PostgreSQL | `localhost` |
| `DWH_PORT` | Порт PostgreSQL | `5432` |
| `DWH_NAME` | Имя базы данных | `dwh` |
| `DWH_USER` | Пользователь БД | `dwh` |
| `DWH_PASSWORD` | Пароль БД | `dwh` |

---

## CI/CD

GitHub Actions запускает три job'а на каждый push в `main` / `develop`:

```
push
 ├── lint (ruff check + ruff format --check)
 ├── test (pytest tests/unit/ -v)
 └── validate-docker (needs: lint, test)
      ├── docker compose config
      └── docker compose build --no-cache
```

Lint и test работают параллельно. Docker-валидация запускается только после их успешного завершения — сборка образа занимает ~10 минут, гонять её при сломанных тестах нет смысла.

---

## Как добавить новый источник данных

Архитектура расширяется по единой схеме — каждый шаг изолирован:

- [ ] Создать Pydantic-схему в `src/ingestion/schemas/`
- [ ] Реализовать наследника `BaseProducer` в `src/ingestion/producers/`
- [ ] Реализовать consumer в `src/ingestion/consumers/`
- [ ] Добавить Kafka-топик в `docker-compose.yml` (секция `kafka-init`)
- [ ] Создать Hub и Satellite в `liquibase/changelog/`
- [ ] Реализовать Spark-джоб наследник `SparkJob` в `src/processing/raw_to_vault/`
- [ ] Добавить task в Airflow DAG
- [ ] Написать unit-тесты с реальной фикстурой API-ответа

---

## Что планируется

- [ ] Great Expectations для контроля качества данных в raw-слое
- [ ] dbt для трансформаций в витрины вместо raw SQL
- [ ] Streaming-режим через Spark Structured Streaming
- [ ] Интеграция с Yahoo Finance API как дополнительный источник котировок
- [ ] Алерты в Airflow при отклонении объёма данных от нормы
