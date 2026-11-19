# TASKS.md — Ingestion

> Read `../TASKS.md` first for platform-wide rules. This file drives all implementation work for the HR ingestion pipeline.

## What this project builds

A production-grade HR data ingestion pipeline with three source connectors, a layered Postgres data model, dbt transformations, and Trino OLAP access.

This is the foundational module. Every other module depends on the schemas, roles, and data it creates.

## Directory structure

```
ingestion/
├── TASKS.md
├── README.md
├── Makefile
├── pyproject.toml
├── src/
│   ├── __init__.py
│   ├── connectors/             # API connectors
│   │   ...
│   ├── models/                 # Pydantic model
│   │   ...
│   └── utils/                  # Utils (data generator, schema drift detector, database helpers)
│       ...
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── macros/
│   │   ...
│   ├── models/                 # dbt models
│   │   ├── staging/
│   │   │   ...
│   │   ├── marts/
│   │   │   ...
│   │   └── reports/
│   │       ...
│   └── tests/                  # dbt tests
│       ...
├── airflow/
│   ├── dags/                   # Airflow DAG
│   │   ...
│   └── plugins/
│       ...
├── docker/                     # Mounted into Docker containers
│   ├── init.sql  
│   ├── mock_workday_server.py
│   └── trino/
│       └── catalog/
│           └── postgresql.properties
├── tests/                      # unit and integration tests
│   ├── conftest.py
│   ├── unit/
│   │   ...
│   └── integration/
│       ...
└── .github/
    └── workflows/
        └── ingestion-ci.yml    # CI workflow
```

## Implementation tasks

### Task 1.0 — Postgres initialisation DDL (`docker/init.sql`)

Create the following in order:

```sql
-- 1. Extensions
CREATE EXTENSION ...

-- 2. Schemas
CREATE SCHEMA ...

-- 3. Roles
CREATE ROLE ...

-- 4. Grants
GRANT ...

-- 5. Core raw tables (schema-on-write)
CREATE TABLE IF NOT EXISTS raw.* (
    -- columns
);

-- 6. Indexes
CREATE INDEX ...
```

The `init.sql` file uses Docker's `POSTGRES_*` env substitution. Passwords must be read from environment variables — not hardcoded.

### Task 1.1 — Trino catalog (`docker/trino/catalog/postgresql.properties`)

```properties
connector.name=...
connection-url=...
connection-user=...
connection-password=...
```

Trino must be able to query `analytics.*` tables. For example:

```sql
SELECT * FROM postgresql.analytics.dim_employees LIMIT 10;
```

### Task 1.2 — Pydantic models (`src/models/employee.py`)

Define these models:

```python
class EmployeeRaw(BaseModel):
    ...

class JobApplicationRaw(BaseModel):
    ...
```

### Task 1.3 — Base connector (`src/connectors/base.py`)

```python
class BaseConnector(ABC):
    ...
```

### Task 1.4 — Mock Workday connector (`src/connectors/workday.py`)

The mock Workday connector calls a local Flask server (`docker/mock_workday_server.py`) that returns synthetic JSON. In production, replace the base URL env var to point at the real Workday API.

```python
WORKDAY_FIELD_MAP = {
    ...  # src/dst keys mapping
}

class WorkdayConnector():
    def __init__(
        ...
    ):
        # Read WORKDAY_BASE_URL from env
        self.base_url = ...
        # Read WORKDAY_API_TOKEN from env
        self.api_token = ...
        # Use httpx for HTTP calls — async-compatible
        self._client = ...

    @retry(
        ...  # Retry logic
    )
    def _get():
        ...

    def _paginate():
        # Implement pagination
        ...

    def _map_employee():
        # Map Workday JSON keys to EmployeeRaw fields
        ...

    def fetch_employees():
        ...

    def fetch_job_applications():
        ...
```

### Task 1.5 — Mock Greenhouse connector (`src/connectors/greenhouse.py`)

Same pattern as Workday.

### Task 1.6 — Airtable connector (`src/connectors/airtable.py`)

Uses the real Airtable API (free account).

```python
_RATE_LIMIT_SLEEP = 0.2  # Airtable allows ~5 req/sec.

class AirtableConnector():
    def __init__(
        ...
    ):
        # AIRTABLE_API_KEY from env
        self.api_key = ...
        # AIRTABLE_BASE_ID from env
        self.base_id = ...
        # Table names in Airtable (usef defined)
        self.employees_table = ...
        self.applications_table = ...  # optional

    def is_configured():
        ...

    def _api():
        from pyairtable import Api  # Airtable API Python lib
        ...

    def _iter_records():
        ...
        time.sleep(_RATE_LIMIT_SLEEP)

    def fetch_employees():
        ...

    def fetch_job_applications():
        ...
```

The `README.md` includes instructions for creating the Airtable base with the correct field names.

### Task 1.7 — Schema drift detector (`src/utils/schema_drift.py`)

```python
def detect_drift(
    source: str,
    new_records: list[dict],
    baseline_schema: dict[str, str],   # field_name -> type_hint
    pii_fields: set[str],
) -> list[DriftEvent]:
    """
    Compare field names and value types in new_records against baseline.
    Return list of DriftEvent(field_name, change_type, old_type, new_type, is_pii).
    """
```

Logic:
1. Infer schema from `new_records` (field names + Python type of first non-null value)
2. Compare against `baseline_schema`
3. Detect: added fields, removed fields, type changes
4. Flag any drifted field that appears in `pii_fields`
5. Write results to `raw.schema_drift_log`

### Task 1.8 — Synthetic data generator (`src/utils/synthetic_data.py`) /

Use `faker` to generate realistic employee data for local development.

```python
def generate_employees():
    ...

def generate_job_applications():
    ...
```

This generator is used in:
1. `docker/mock_workday_server.py` (serves fake data via Flask)
2. `tests/conftest.py` (fixtures)
3. Standalone: `python -m ingestion.utils.synthetic_data` to seed the DB directly

### Task 1.9 — Database utilities (`src/utils/db.py`)

```python
def get_connection():
    ...

def upsert_employees():
    ...

def upsert_job_applications():
    ...
```

Use `psycopg2` with `execute_values` for bulk inserts (not row-by-row).

### Task 1.10 — dbt models

**`dbt_project.yml`**:

```yaml
name: workforce_ingestion
version: '1.0.0'
config-version: 2
profile: workforce
model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]
models:
  workforce_ingestion:
    staging:
      +schema: staging
      +materialized: view
    marts:
      +schema: analytics
      +materialized: table
    reports:
      +schema: analytics
      +materialized: table
```

**`models/staging/stg_employees.sql`**:

```sql
-- Flatten JSONB payload from raw.employees into typed columns.
-- Source: raw.employees WHERE source IN ('workday', 'airtable')
-- Cast all fields to proper types.
-- Exclude terminated employees with termination_date > 90 days ago.
-- Add is_active flag: termination_date IS NULL OR termination_date > CURRENT_DATE
```

**`models/staging/stg_job_applications.sql`**:

```sql
-- Flatten raw.job_applications (source: greenhouse, airtable).
-- Parse stage timestamps.
-- Calculate time_in_stage_days for each stage transition.
```

**`models/marts/dim_employees.sql`**:

```sql
-- SCD Type 1 dimension from stg_employees.
-- Columns: employee_id, full_name, email, department, job_title, level,
--          hire_date, termination_date, is_active, employment_type,
--          manager_id, location, created_at, updated_at
-- NOTE: salary and performance_rating are intentionally EXCLUDED.
--       They exist in raw but are masked here. Governance module adds them
--       as restricted columns accessible only to the hr_partner role.
```

**`models/marts/fct_headcount_daily.sql`**:

```sql
-- Spine: generate_series over the past 2 years, daily.
-- For each date: count of active employees by department, level, employment_type.
-- Join against dim_employees using hire_date <= spine_date AND
-- (termination_date IS NULL OR termination_date > spine_date).
```

**`models/marts/fct_attrition_monthly.sql`**:

```sql
-- Monthly attrition rate = terminations in month / avg headcount in month.
-- Columns: year_month, department, voluntary_terminations,
--          involuntary_terminations, total_terminations, avg_headcount,
--          attrition_rate_pct, rolling_12m_attrition_rate_pct
```

**`models/reports/rpt_recruiting_funnel.sql`**:

```sql
-- From stg_job_applications.
-- Columns: job_id, job_title, department, applied_count, phone_screen_count,
--          interview_count, offer_count, hired_count,
--          application_to_hire_days_avg, offer_acceptance_rate_pct
-- Grain: one row per job_id per month.
```

**dbt tests to implement** (in `schema.yml`):
- `not_null` on all primary keys
- `unique` on `dim_employees.employee_id`
- `accepted_values` on `employment_type` and `level`
- Custom tests: `assert_headcount_positive` (headcount never negative) and `assert_no_null_employee_id` (`dim_employees` must never contain a null `employee_id`)

### Task 1.11 — Airflow DAG (`airflow/dags/hr_ingestion_dag.py`)

```python
from airflow.decorators import dag, task

@dag(
    ...
)
def hr_ingestion_dag():
    @task
    def extract_workday() -> dict:
        """Run WorkdayConnector, upsert to raw.employees. Return stats dict."""

    @task
    def extract_greenhouse() -> dict:
        """Run GreenhouseConnector, upsert to raw.job_applications. Return stats dict."""

    @task
    def extract_airtable() -> dict:
        """Run AirtableConnector, upsert to raw.employees + raw.job_applications."""

    @task
    def detect_schema_drift(workday_stats: dict, airtable_stats: dict) -> list:
        """Run schema drift detector against new records. Return list of drift events."""

    @task
    def run_dbt_models() -> None:
        """Run dbt using dbt-core Python API (not BashOperator)."""
        # from dbt.cli.main import dbtRunner
        # dbtRunner().invoke(["run", "--project-dir", ...])

    @task
    def alert_on_pii_change(drift_events: list) -> None:
        """If any drift_event.is_pii is True, send Slack alert via SLACK_WEBHOOK_URL."""

    @task
    def trigger_llm_eval(**context) -> None:
        """TriggerDagRunOperator pattern — kick off llm_eval_embedding_refresh."""

    # Wire tasks
    workday = extract_workday()
    greenhouse = extract_greenhouse()
    airtable = extract_airtable()
    drift = detect_schema_drift(workday, airtable)
    dbt = run_dbt_models()
    alert = alert_on_pii_change(drift)
    trigger = trigger_llm_eval()

    [workday, greenhouse, airtable] >> drift >> dbt >> alert >> trigger

hr_ingestion_dag()
```

### Task 1.12 — Mock Workday Flask server (`docker/mock_workday_server.py`)

```python
from flask import Flask, jsonify, request
from src.utils.synthetic_data import generate_employees, generate_job_applications

app = Flask(__name__)

# Generate data once at startup
EMPLOYEES = generate_employees(500)
APPLICATIONS = generate_job_applications(EMPLOYEES, 1000)

@app.route("/api/v1/workers")
def get_workers():
    """Paginated endpoint. ?page=1&page_size=100"""
    # Implement pagination with _links.next

@app.route("/api/v1/workers/<worker_id>")
def get_worker(worker_id: str):
    """Single worker by ID."""

if __name__ == "__main__":
    app.run(port=5001, debug=False)
```

### Task 1.13 — Tests

**`tests/unit/test_workday_connector.py`**:
- Mock `httpx.get` to return fixture JSON
- Assert `fetch_employees()` yields correct `EmployeeRaw` objects
- Assert retry logic fires on 429
- Assert pagination follows `_links.next`

**`tests/unit/test_schema_drift.py`**:
- Test: no drift on identical schema
- Test: new field detected as `added`
- Test: missing field detected as `removed`
- Test: PII field drift sets `is_pii=True`

**`tests/integration/test_postgres_ingestion.py`**:
- Spin up Postgres via `testcontainers`
- Run `init.sql`
- Upsert 10 synthetic employees
- Assert `raw.employees` count = 10
- Upsert same 10 again — assert count still = 10 (upsert idempotency)

**`tests/integration/test_trino_queries.py`**:
- Requires running Trino (mark with `@pytest.mark.integration`)
- Connect via `trino` Python client
- Assert `SELECT COUNT(*) FROM postgresql.analytics.dim_employees` returns > 0

### Task 1.14 — `README.md`

Include:
1. One-paragraph purpose statement
2. ASCII architecture diagram showing: sources → connectors → Postgres → dbt → Trino
3. Tech stack table: Language, Framework, DB, Orchestration, Testing
4. Setup instructions (step-by-step, numbered)
5. Airtable base setup instructions (field names, types — so recruiter can recreate)
6. `make` targets table
7. dbt model descriptions (one line each)
8. Design decisions:

    - Why `upsert` over `truncate-load`
    - Why `salary`/`performance_rating` are excluded from `dim_employees`
    - Why Trino sits on top of Postgres (OLTP vs OLAP access patterns)
    - Why mock servers over static fixtures

### Task 1.15 — `pyproject.toml`

```toml
[project]
name = "wip-ingestion"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    ...
]

[project.optional-dependencies]
dev = [
    ...
]
```

### Task 1.16 — `Makefile`

```Makefile
.PHONY: setup seed dbt-run test test-unit test-integration lint

setup:
	pip install -e ".[dev]"
	python docker/mock_workday_server.py &
	python -m ingestion.utils.synthetic_data seed

seed:
	python -m ingestion.utils.synthetic_data seed

dbt-run:
	cd dbt && dbt deps && dbt run --profiles-dir .

test-unit:
	pytest tests/unit/ -v --cov=src --cov-report=term-missing

test-integration:
	pytest tests/integration/ -v -m integration

test: test-unit test-integration

lint:
	ruff check src/
	sqlfluff lint dbt/models --dialect postgres
```

## Acceptance criteria

- `make setup && make seed` completes without errors
- `raw.employees` contains >= 500 rows after seed
- `make dbt-run` completes with 0 model errors
- `analytics.dim_employees` is queryable via Trino
- `make test-unit` passes with ≥ 80% coverage
- Airflow DAG `hr_ingestion` visible in UI, runs end-to-end on manual trigger
- Schema drift log writes a row when a new field appears in source data
- PII field drift triggers a Slack alert (verify with a test field named `ssn`)
