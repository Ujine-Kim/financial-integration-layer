# Claude Code — План разработки Financial Integration Layer

Каждый блок ниже — это готовый промпт для Claude Code.
Копируй и вставляй последовательно. Не переходи к следующему этапу,
пока не проверил результат предыдущего.

---

## ИНИЦИАЛИЗАЦИЯ (один раз, в самом начале)

```
Мы разрабатываем pet-проект "Financial Integration Layer" —
банковский интеграционный слой для загрузки финансовых данных
из публичных API в хранилище по архитектуре Data Vault 2.0.

Технологический стек:
- Apache Kafka 3.7 (KRaft) — стриминг сырых событий
- Apache Spark (PySpark) — batch ETL обработка
- Apache Airflow 2.9 — оркестрация пайплайнов
- PostgreSQL 16 — DWH (схемы: raw, vault, dm)
- MinIO — S3-совместимое хранилище (зоны: raw-zone, vault-zone, dm-zone)
- Python 3.11 — продюсеры, трансформации, утилиты

Источники данных:
- Alpha Vantage API — биржевые котировки (бесплатный ключ)
- ExchangeRate API — валютные курсы (бесплатный ключ)
- CBR XML API — ставки ЦБ РФ (без ключа, публичный)

Инфраструктура уже поднята через docker-compose.yml.
Kafka топики уже созданы: quotes.raw, rates.raw, instruments.raw, cbr.rates.raw.
PostgreSQL уже содержит схемы: raw, vault, dm в базе dwh.

Стандарты кода:
- Все Spark jobs наследуются от базового класса SparkJob
- Все DAG-и наследуются от базового класса BaseDag
- Хэш-ключи в Data Vault — MD5 от business key
- Идемпотентная загрузка везде (DELETE+INSERT или partition overwrite)
- Логирование через стандартный logging, не print
- Типизация через type hints везде
- Docstrings на публичных методах и классах

Прочитай структуру проекта и docker-compose.yml, чтобы понять
что уже есть, прежде чем что-то создавать.
```

---

## ЭТАП 2 — Python Ingestion Service

### 2.1 — Зависимости и структура

```
Создай файлы для этапа Ingestion:

1. src/requirements.txt со всеми зависимостями:
   - confluent-kafka (Kafka producer/consumer)
   - requests (HTTP вызовы к API)
   - pydantic v2 (валидация входящих данных)
   - python-dotenv (загрузка .env)
   - tenacity (retry логика для API вызовов)
   - structlog (структурированное логирование)
   - pytest, pytest-mock (тесты)

2. src/__init__.py — пустой

3. .env.example — шаблон переменных окружения:
   ALPHA_VANTAGE_API_KEY=your_key_here
   EXCHANGE_RATE_API_KEY=your_key_here
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   MINIO_ENDPOINT=http://localhost:9000
   MINIO_ACCESS_KEY=minioadmin
   MINIO_SECRET_KEY=minioadmin123
   DWH_HOST=localhost
   DWH_PORT=5432
   DWH_NAME=dwh
   DWH_USER=dwh
   DWH_PASSWORD=dwh

4. src/common/config.py — класс Settings через pydantic BaseSettings,
   читает все переменные из .env
```

### 2.2 — Pydantic схемы входящих данных

```
Создай src/ingestion/schemas/ с Pydantic v2 моделями для валидации
входящих данных от каждого API:

1. src/ingestion/schemas/quote.py — модель RawQuote:
   - symbol: str
   - open: Decimal
   - high: Decimal
   - low: Decimal
   - close: Decimal
   - volume: int
   - trade_date: date
   - source: str  # "alpha_vantage"
   - loaded_at: datetime  # utcnow при создании

2. src/ingestion/schemas/rate.py — модель RawRate:
   - base_currency: str  # "USD"
   - target_currency: str  # "RUB"
   - rate: Decimal
   - rate_date: date
   - source: str  # "exchange_rate_api" или "cbr"
   - loaded_at: datetime

3. src/ingestion/schemas/instrument.py — модель RawInstrument:
   - symbol: str
   - name: str
   - exchange: str
   - asset_type: str  # "Stock", "ETF" и т.д.
   - source: str
   - loaded_at: datetime

Все модели должны сериализоваться в JSON через model.model_dump_json().
Decimal поля должны сериализоваться как строки (для точности).
```

### 2.3 — Базовый Kafka Producer

```
Создай src/ingestion/producers/base_producer.py —
абстрактный базовый класс BaseProducer:

- В конструкторе принимает topic: str и bootstrap_servers: str
- Инициализирует confluent_kafka.Producer с настройками:
  - compression.type: gzip
  - acks: all
  - retries: 3
- Метод produce(key: str, value: str) — отправляет сообщение
- Метод flush() — сбрасывает буфер
- Абстрактный метод fetch_and_produce() — реализуется в наследниках
- Метод on_delivery(err, msg) — callback для логирования результата доставки
- Context manager (__enter__ / __exit__) — flush при выходе
- Все ошибки логируются через structlog, не бросаются наружу без обёртки
```

### 2.4 — Продюсер Alpha Vantage

```
Создай src/ingestion/producers/alpha_vantage_producer.py —
класс AlphaVantageProducer(BaseProducer):

API документация: https://www.alphavantage.co/documentation/
Используем endpoint: TIME_SERIES_DAILY (function=TIME_SERIES_DAILY)
Бесплатный план: 25 запросов в день, 1 символ за раз.

Логика:
- Принимает список symbols: list[str] (например ["AAPL", "MSFT", "GOOGL"])
- Для каждого символа делает GET запрос к Alpha Vantage
- Парсит ответ, создаёт список RawQuote объектов
- Берём только последнюю дату из ответа (latest available)
- Валидирует через Pydantic, логирует ошибки валидации
- Продюсирует в топик quotes.raw
- Ключ сообщения: "{symbol}:{trade_date}"
- Retry через tenacity: 3 попытки, exponential backoff, только на HTTP 5xx и ConnectionError

Добавь функцию main() для запуска вручную:
  python -m src.ingestion.producers.alpha_vantage_producer
```

### 2.5 — Продюсер CBR (ЦБ РФ)

```
Создай src/ingestion/producers/cbr_producer.py —
класс CBRProducer(BaseProducer):

ЦБ РФ публикует XML с курсами валют без авторизации:
URL: https://www.cbr.ru/scripts/XML_daily.asp
Параметр date_req для конкретной даты: ?date_req=02/03/2002

Логика:
- Скачивает XML за сегодня (без параметра = текущий день)
- Парсит через xml.etree.ElementTree (без внешних зависимостей)
- Создаёт RawRate объекты для каждой валюты в XML
- base_currency всегда "RUB" (ЦБ РФ даёт курс к рублю)
- Продюсирует в топик cbr.rates.raw
- Ключ: "{currency_code}:{rate_date}"

Добавь функцию main() для ручного запуска.
```

### 2.6 — Тесты для продюсеров

```
Создай tests/unit/ingestion/ с unit-тестами:

1. tests/unit/ingestion/test_schemas.py
   - Тест успешной валидации корректных данных для каждой схемы
   - Тест что Decimal поля не теряют точность
   - Тест что невалидные данные бросают ValidationError

2. tests/unit/ingestion/test_cbr_producer.py
   - Замокай requests.get через pytest-mock
   - Верни тестовый XML (положи фикстуру в tests/fixtures/cbr_response.xml)
   - Проверь что создаётся правильное количество RawRate объектов
   - Проверь что ключ сообщения формируется корректно

3. tests/unit/ingestion/test_alpha_vantage_producer.py
   - Замокай requests.get
   - Верни тестовый JSON ответ (фикстура в tests/fixtures/alpha_vantage_response.json)
   - Проверь парсинг и валидацию

Запускай через: pytest tests/unit/ -v
```

---

## ЭТАП 3 — Data Vault DDL + Spark ETL

### 3.1 — Liquibase миграции (Data Vault схема)

```
Создай Liquibase миграции для Data Vault слоя в PostgreSQL.
Директория: liquibase/changelog/

Структура файлов:
liquibase/
├── liquibase.properties        # подключение к dwh БД
├── changelog/
│   ├── db.changelog-master.xml # мастер-файл, включает все остальные
│   ├── v001_create_hubs.sql
│   ├── v002_create_links.sql
│   └── v003_create_satellites.sql

Таблицы для создания (схема vault):

HUBS (бизнес-ключи, без истории):
- vault.hub_instrument(hub_instrument_hk, instrument_bk, load_date, record_source)
- vault.hub_currency(hub_currency_hk, currency_code, load_date, record_source)
- vault.hub_market(hub_market_hk, market_code, load_date, record_source)

LINKS (связи между хабами):
- vault.lnk_quote_instrument(lnk_quote_instrument_hk, hub_instrument_hk, hub_market_hk, load_date, record_source)
- vault.lnk_rate_currency(lnk_rate_currency_hk, hub_currency_hk_base, hub_currency_hk_target, load_date, record_source)

SATELLITES (атрибуты с историей, hash diff для дедупликации):
- vault.sat_instrument_details(hub_instrument_hk, load_date, load_end_date, record_source, hash_diff, instrument_name, exchange, asset_type)
- vault.sat_quote_prices(hub_instrument_hk, load_date, record_source, hash_diff, open_price, high_price, low_price, close_price, volume, trade_date)
- vault.sat_rate_values(lnk_rate_currency_hk, load_date, record_source, hash_diff, rate, rate_date)

Правила:
- Все _hk поля: CHAR(32) NOT NULL (MD5 хэш)
- load_date: TIMESTAMP NOT NULL DEFAULT NOW()
- load_end_date: TIMESTAMP NULL (NULL = текущая запись)
- record_source: VARCHAR(100) NOT NULL
- hash_diff: CHAR(32) NOT NULL (MD5 от атрибутов для delta-load)
- PRIMARY KEY на hk + load_date для satellites
- Индексы на все FK поля
```

### 3.2 — Базовый класс Spark Job

```
Создай src/processing/base/spark_job.py —
абстрактный базовый класс SparkJob:

- Конструктор принимает job_name: str, app_name: str | None = None
- Метод _create_spark_session() — создаёт SparkSession с настройками:
  - master: local[*] для разработки (через env переменную SPARK_MASTER)
  - конфиги для подключения к MinIO как S3:
    fs.s3a.endpoint = MINIO_ENDPOINT
    fs.s3a.access.key = MINIO_ACCESS_KEY
    fs.s3a.secret.key = MINIO_SECRET_KEY
    fs.s3a.path.style.access = true
  - spark.sql.extensions для Iceberg (опционально через env флаг)
- Абстрактный метод run() -> None — основная логика джоба
- Метод execute() — оборачивает run() в try/finally, гарантирует spark.stop()
- Метод _get_jdbc_options(schema: str) — возвращает dict с JDBC настройками для PostgreSQL
- Логирование через structlog с job_name в каждом сообщении
- Context manager поддержка
```

### 3.3 — Утилиты Data Vault

```
Создай src/common/vault_utils.py с утилитами для Data Vault:

1. Функция generate_hash_key(business_key: str) -> str
   - MD5 от бизнес-ключа в uppercase
   - Пример: generate_hash_key("AAPL") -> "ABC123..."

2. Функция generate_hash_key_composite(*keys: str) -> str
   - MD5 от конкатенации ключей через "||"
   - Пример: generate_hash_key_composite("AAPL", "NASDAQ")

3. Функция generate_hash_diff(df: DataFrame, columns: list[str]) -> DataFrame
   - Добавляет колонку hash_diff — MD5 от конкатенации значений указанных колонок
   - Используется для delta-load в Satellite

4. Функция add_vault_metadata(df: DataFrame, record_source: str) -> DataFrame
   - Добавляет колонки: load_date (current_timestamp), record_source

Все функции работают с PySpark DataFrame и Column API,
без использованием UDF там где возможно (используй pyspark.sql.functions).
```

### 3.4 — Spark job: Raw → Hub Instrument

```
Создай src/processing/raw_to_vault/load_hub_instrument.py —
класс LoadHubInstrument(SparkJob):

Логика (идемпотентная загрузка):
1. Читает данные из PostgreSQL таблицы raw.quotes (или из Kafka топика quotes.raw)
   За последние N часов (параметр execution_date передаётся через аргументы)
2. Извлекает уникальные символы (business key = symbol)
3. Генерирует hub_instrument_hk через generate_hash_key(symbol)
4. Добавляет load_date, record_source = "alpha_vantage"
5. Загружает в vault.hub_instrument через INSERT ... ON CONFLICT DO NOTHING
   (идемпотентность: если хэш уже есть — пропускаем)

Используй DataFrame API (не SQL строки), типизированные схемы.
Добавь if __name__ == "__main__": для ручного запуска.
```

### 3.5 — Spark job: Raw → Satellite Quote Prices

```
Создай src/processing/raw_to_vault/load_sat_quote_prices.py —
класс LoadSatQuotePrices(SparkJob):

Логика (delta-load через hash_diff):
1. Читает новые котировки из raw слоя за дату execution_date
2. Джойнит с vault.hub_instrument для получения hub_instrument_hk
3. Генерирует hash_diff от полей: open, high, low, close, volume
4. Сравнивает с уже загруженными записями в sat_quote_prices:
   - Если hash_diff совпадает для того же hk + trade_date — пропускаем
   - Если новый или изменился — вставляем новую строку
5. Загружает через JDBC в vault.sat_quote_prices

Это классический delta-load паттерн для Satellite в Data Vault.
```

---

## ЭТАП 4 — Airflow DAGs + витрины

### 4.1 — Базовый класс DAG

```
Создай airflow/dags/base/base_dag.py —
базовый класс и фабричная функция для стандартизации DAG-ов:

1. Функция create_default_args(owner: str = "fil-team") -> dict
   - retries: 2
   - retry_delay: timedelta(minutes=5)
   - email_on_failure: False
   - depends_on_past: False
   - owner: owner

2. Функция create_dag(dag_id, schedule, description, tags) -> DAG
   - Применяет create_default_args()
   - catchup=False всегда
   - Добавляет тег "financial-integration-layer" ко всем тегам
   - doc_md с описанием

3. Декоратор @register_dag — опционально, для автодискавери

Цель: все DAG-и в проекте выглядят единообразно,
стандарты не разбросаны по файлам.
```

### 4.2 — DAG: Ingestion Pipeline

```
Создай airflow/dags/dag_ingest_quotes.py —
DAG для ежедневной загрузки котировок:

dag_id: "ingest_quotes_daily"
schedule: "0 18 * * 1-5"  # по будням в 18:00 (после закрытия NYSE)

Задачи (tasks):
1. check_api_availability — HttpSensor или PythonSensor
   Проверяет что Alpha Vantage API доступен перед стартом

2. fetch_alpha_vantage — PythonOperator
   Запускает AlphaVantageProducer для списка символов из Variable "WATCH_SYMBOLS"
   (["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"])

3. fetch_cbr_rates — PythonOperator
   Запускает CBRProducer параллельно с fetch_alpha_vantage

4. validate_kafka_messages — PythonSensor
   Проверяет что в топике quotes.raw появились сообщения за сегодня
   mode="reschedule" (не блокирует воркер)

5. trigger_spark_etl — TriggerDagRunOperator
   Запускает dag_id="spark_raw_to_vault" после успешной валидации

Зависимости:
check_api_availability >> [fetch_alpha_vantage, fetch_cbr_rates] >> validate_kafka_messages >> trigger_spark_etl
```

### 4.3 — DAG: Spark ETL (Raw → Vault)

```
Создай airflow/dags/dag_spark_raw_to_vault.py —
DAG для запуска Spark ETL jobs:

dag_id: "spark_raw_to_vault"
schedule: None  # запускается только через TriggerDagRunOperator

Задачи:
1. load_hub_instrument — BashOperator или PythonOperator
   Запускает src/processing/raw_to_vault/load_hub_instrument.py
   с параметром execution_date из context

2. load_hub_currency — аналогично для валют

3. load_sat_quote_prices — после load_hub_instrument
   Запускает load_sat_quote_prices.py

4. load_sat_rate_values — после load_hub_currency

5. validate_vault_counts — PythonOperator
   SQL запрос к vault схеме, проверяет что количество записей > 0
   Бросает AirflowSkipException если данных нет (не фейлит DAG)

Зависимости:
load_hub_instrument >> load_sat_quote_prices >> validate_vault_counts
load_hub_currency >> load_sat_rate_values >> validate_vault_counts
```

### 4.4 — DAG: Build Data Marts

```
Создай airflow/dags/dag_build_marts.py —
DAG для построения витрин BI.DM:

dag_id: "build_dm_marts"
schedule: "0 20 * * 1-5"  # после завершения ETL

SQL витрины создавать как файлы в sql/marts/:

1. sql/marts/dm_quotes_daily.sql — витрина котировок:
   - Джойнит sat_quote_prices + hub_instrument + sat_instrument_details
   - Добавляет расчётные поля: daily_return = (close - open) / open * 100
   - Загружает в dm.dm_quotes_daily через DELETE WHERE trade_date = :date + INSERT
   - Партиционирование по trade_date

2. sql/marts/dm_fx_rates.sql — витрина валютных курсов:
   - Кросс-курсы через ЦБ РФ и ExchangeRate API
   - dm.dm_fx_rates

Задачи DAG:
1. truncate_and_load_dm_quotes — PostgresOperator с dm_quotes_daily.sql
2. truncate_and_load_dm_fx_rates — PostgresOperator с dm_fx_rates.sql
3. update_refresh_log — PythonOperator, пишет в лог-таблицу время обновления витрины

Идемпотентность: DELETE + INSERT по дате, можно перезапускать без дублей.
```

---

## ЭТАП 5 — CI/CD и качество кода

### 5.1 — Конфигурация линтеров

```
Создай файлы конфигурации для качества кода:

1. pyproject.toml в корне проекта:
   [tool.ruff]
   line-length = 100
   select = ["E", "F", "I", "UP", "B"]
   
   [tool.mypy]
   python_version = "3.11"
   ignore_missing_imports = true
   
   [tool.pytest.ini_options]
   testpaths = ["tests"]
   python_files = "test_*.py"
   python_functions = "test_*"

2. .gitignore для Python + Airflow проекта:
   __pycache__/, *.pyc, .env, airflow/logs/*, .ruff_cache/,
   .mypy_cache/, .pytest_cache/, *.egg-info/, dist/

3. Добавь ruff в requirements.txt (dev зависимости)
```

### 5.2 — GitHub Actions CI

```
Создай .github/workflows/ci.yml — пайплайн CI:

Триггеры: push на main и develop, pull_request на main

Jobs:

1. lint (ubuntu-latest, python 3.11):
   - Установка ruff
   - ruff check src/ tests/ airflow/dags/
   - ruff format --check src/ tests/

2. test (ubuntu-latest, python 3.11):
   - Установка зависимостей из src/requirements.txt
   - pytest tests/unit/ -v --tb=short
   - Загрузка coverage репорта (опционально)

3. validate-docker (ubuntu-latest):
   - docker compose config  # проверяет синтаксис docker-compose.yml
   - docker compose build --no-cache  # проверяет что образы собираются

Jobs выполняются параллельно (lint и test), validate-docker после них.
Используй кеширование pip через actions/cache.
```

### 5.3 — Финальная документация

```
Обнови README.md в корне проекта — добавь секции:

1. Архитектура — ASCII или Mermaid диаграмма всего пайплайна:
   Sources → Kafka → Spark → Data Vault → DM → BI

2. Быстрый старт — пошаговые команды от git clone до первого запуска DAG

3. Переменные окружения — таблица всех .env переменных с описанием

4. Структура Data Vault — описание Hub/Link/Satellite с примером для нашей модели

5. Kafka топики — таблица с топиками, партициями, политикой

6. Как добавить новый источник данных — чеклист:
   [ ] Создать Pydantic схему в src/ingestion/schemas/
   [ ] Создать продюсер наследник BaseProducer
   [ ] Добавить Kafka топик в docker-compose.yml (kafka-init)
   [ ] Создать Hub и Satellite в Liquibase миграции
   [ ] Создать Spark job наследник SparkJob
   [ ] Добавить task в Airflow DAG

7. CI/CD badges в шапке README
```

---

## ПОРЯДОК ВЫПОЛНЕНИЯ

```
Этап 2.1 → 2.2 → 2.3 → 2.4 → 2.5 → 2.6
             ↓
Этап 3.1 → 3.2 → 3.3 → 3.4 → 3.5
             ↓
Этап 4.1 → 4.2 → 4.3 → 4.4
             ↓
Этап 5.1 → 5.2 → 5.3
```

После каждого этапа запускай:
```bash
# Проверка синтаксиса
ruff check src/ tests/

# Юнит тесты
pytest tests/unit/ -v

# Что поднято в docker
make ps
```

---

## ВОПРОСЫ КОТОРЫЕ МОЖЕТ ЗАДАТЬ CLAUDE CODE

Если Claude Code спросит уточнения — вот готовые ответы:

- **"Какой Python версии?"** → 3.11
- **"Spark локально или в кластере?"** → local[*] для разработки, настройки через env
- **"Iceberg или обычный Parquet?"** → обычный Parquet для MVP, Iceberg — комментарий TODO
- **"Как запускать Spark jobs из Airflow?"** → BashOperator с spark-submit или PythonOperator
- **"Нужен ли Alembic?"** → нет, только Liquibase для DDL миграций
- **"Формат дат в Data Vault?"** → TIMESTAMP WITH TIME ZONE, UTC везде
