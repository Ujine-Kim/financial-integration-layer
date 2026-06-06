# Financial Integration Layer

![CI](https://github.com/your-org/financial-integration-layer/actions/workflows/ci.yml/badge.svg)

Банковский интеграционный слой для загрузки финансовых данных из публичных API
в хранилище по архитектуре **Data Vault 2.0**.

---

## Архитектура

```
┌─────────────────┐    ┌─────────┐    ┌───────────┐    ┌──────────────┐    ┌────┐
│  Alpha Vantage  │───▶│         │    │           │    │  Data Vault  │    │    │
│  (котировки)    │    │  Kafka  │───▶│   Spark   │───▶│  vault.*     │───▶│ DM │
│  CBR XML        │───▶│  topics │    │   ETL     │    │  (Hub/Link/  │    │    │
│  (курсы ЦБ РФ) │    │         │    │           │    │   Satellite) │    │    │
└─────────────────┘    └─────────┘    └───────────┘    └──────────────┘    └────┘
       Python              Kafka          PySpark          PostgreSQL         BI
      Producers            KRaft          Jobs              vault             dm
                                                            schema           schema

                    MinIO (S3) ──── промежуточные файлы Spark
                    Airflow    ──── оркестрация пайплайнов
```

---

## Быстрый старт

### 1. Клонируем репозиторий
```bash
git clone https://github.com/your-org/financial-integration-layer.git
cd financial-integration-layer
```

### 2. Настраиваем переменные окружения
```bash
cp .env.example .env
# Вписываем ключи API в .env:
# ALPHA_VANTAGE_API_KEY=ваш_ключ  (https://www.alphavantage.co/support/#api-key)
# EXCHANGE_RATE_API_KEY=ваш_ключ  (https://app.exchangerate-api.com/)
```

### 3. Поднимаем инфраструктуру
```bash
docker compose up -d postgres kafka minio
docker compose up kafka-init minio-init   # создаём топики и бакеты
docker compose up -d airflow-webserver airflow-scheduler
```

### 4. Применяем DDL миграции
```bash
# Устанавливаем Liquibase (https://www.liquibase.com/download)
cd liquibase
liquibase update
```

### 5. Устанавливаем Python зависимости
```bash
pip install -r src/requirements.txt
```

### 6. Запускаем тесты
```bash
python -m pytest tests/unit/ -v
```

### 7. Открываем Airflow UI
```
http://localhost:8080
Логин: admin / admin
```

### 8. Запускаем первый DAG вручную
В UI находим `ingest_quotes_daily` → нажимаем ▶ Trigger DAG.

---

## Переменные окружения

| Переменная | Описание | Пример |
|---|---|---|
| `ALPHA_VANTAGE_API_KEY` | Ключ Alpha Vantage API | `ABCD1234` |
| `EXCHANGE_RATE_API_KEY` | Ключ ExchangeRate API | `xyz789` |
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

## Kafka топики

| Топик | Партиции | Описание | Ключ сообщения |
|---|---|---|---|
| `quotes.raw` | 3 | Биржевые котировки (Alpha Vantage) | `{SYMBOL}:{YYYY-MM-DD}` |
| `rates.raw` | 3 | Валютные курсы (ExchangeRate API) | `{CCY}:{YYYY-MM-DD}` |
| `cbr.rates.raw` | 3 | Курсы ЦБ РФ | `{CCY}:{YYYY-MM-DD}` |
| `instruments.raw` | 3 | Справочник инструментов | `{SYMBOL}` |

---

## Структура Data Vault

Data Vault 2.0 делит данные на три типа таблиц:

**Hub** — бизнес-ключи, без истории, только растут:
```
hub_instrument: AAPL, MSFT, GOOGL...
hub_currency:   USD, RUB, EUR...
hub_market:     NASDAQ, NYSE...
```

**Link** — связи между хабами, тоже только растут:
```
lnk_quote_instrument: AAPL торгуется на NASDAQ
lnk_rate_currency:    USD → RUB курс существует
```

**Satellite** — атрибуты с полной историей изменений:
```
sat_quote_prices:     цена AAPL за каждую дату
sat_instrument_details: название, биржа, тип (меняется редко)
sat_rate_values:      значение курса за каждую дату
```

Новая строка в Satellite вставляется только если `hash_diff` изменился — это **delta-load**.

---

## Как добавить новый источник данных

- [ ] Создать Pydantic схему в `src/ingestion/schemas/`
- [ ] Создать продюсер-наследник `BaseProducer` в `src/ingestion/producers/`
- [ ] Добавить Kafka топик в `docker-compose.yml` (секция `kafka-init`)
- [ ] Создать Hub и Satellite таблицы в `liquibase/changelog/`
- [ ] Создать Spark job-наследник `SparkJob` в `src/processing/raw_to_vault/`
- [ ] Добавить task в соответствующий Airflow DAG
- [ ] Написать unit тесты

---

## Структура проекта

```
.
├── airflow/dags/           # Airflow DAG-и
│   ├── base/               # Базовые утилиты для DAG-ов
│   ├── dag_ingest_quotes.py
│   ├── dag_spark_raw_to_vault.py
│   └── dag_build_marts.py
├── src/
│   ├── common/             # Общие утилиты (config, vault_utils)
│   ├── ingestion/          # Python продюсеры для Kafka
│   │   ├── schemas/        # Pydantic модели входящих данных
│   │   └── producers/      # Продюсеры (Alpha Vantage, CBR)
│   └── processing/         # Spark ETL jobs
│       ├── base/           # Базовый класс SparkJob
│       └── raw_to_vault/   # Jobs: raw → vault
├── liquibase/              # DDL миграции Data Vault
├── sql/marts/              # SQL витрин
├── tests/
│   ├── fixtures/           # Тестовые данные (XML, JSON)
│   └── unit/               # Unit тесты
├── docker-compose.yml
└── pyproject.toml
```
