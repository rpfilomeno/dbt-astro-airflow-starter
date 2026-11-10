# dbt-astro-airflow-starter

Astro (Astronomer) Airflow 3 project running in Docker, wrapping two dbt projects:

- **`jaffle_shop`** — the dbt example: seeds drop/rebuild and a `DbtDag`, asset-scheduled off `SEED://JAFFLE_SHOP`.
- **`philvolcs`** — a PHIVOLCS earthquake flash-report pipeline (see below).

Airflow runs on Astro Runtime `3.3-12` via the Astro CLI (managed by **mise**, `mise.toml` → `astro = "latest"`). No CI, linter, or formatter — verification is `astro dev parse` + `astro dev pytest`.

## Quick start

```bash
astro dev start            # build + start (slow first time; --wait 5m)
astro dev pytest           # run the DAG tests (host Python has no Airflow)
astro dev parse            # DAG parse validation
astro dev stop | kill      # stop / tear down
```

`astro dev start` imports `airflow_settings.yaml` (gitignored): the `postgres` connection, the `bigquery-n8n` connection (service-account `keyfile_dict`), and the `GEMINIKEYS` variable (comma-separated Gemini API keys). Copy `airflow_settings.sample.yml` to `airflow_settings.yaml` and fill in real values; edit that file and restart to change them.

New DAGs are created **paused** — unpause once: `astro dev run dags unpause <dag_id>`.

## PHIVOLCS pipeline (`dags/philvolcs.py`)

Scrape the PHIVOLCS latest-earthquake table → Postgres → dbt staging → Gemini structured-output flash report → BigQuery (`n8n-rogverse.philvolcs.raw_llm_flash_reports`; dbt then materializes `philvolcs.flash_reports`).

- **Manual trigger only** (`airflow dags trigger philvolcs`), 4 tasks: `extract_earthquakes` → `dbt_staging` → `generate_flash_report` → `dbt_flash_reports`.
- **LLM call**: pydantic-ai (`pydantic-ai-slim[google]`) — `FlashReport(BaseModel)` as `output_type`, one `GoogleModel` call per attempt, no inner retries.
- **Rotation**: ported from [phsim-astro `attemptLlm`](https://github.com/rpfilomeno/phsim-astro/blob/main/scripts/generate-site-data.ts) — 404/429/502/503 skip the model without burning one of 3 attempts, the first timeout per run is refunded, 429s strike keys and models, micro-backoff between burned tries. Keys are logged by index only; `?key=` is scrubbed from error text.
- **Dedup**: events that already have a report in BigQuery are skipped.
- **dbt targets**: `postgres` for staging models, `bigquery` for `flash_reports` only — never run a full `dbt run` on one target.

### Notable gotchas (details in `AGENTS.md`)

- PHIVOLCS serves only its leaf cert → `include/gsrsaovsslca2018.pem` is concatenated with certifi (`_ca_bundle()`).
- This BigQuery project is free tier → load jobs only (no streaming inserts), and BigQuery rejects FLOAT64 window `PARTITION BY` (the model partitions by `event_time`).
- Postgres `to_timestamp` + `HH12` mis-parses `10:xx` times unless the time part is split off first.

## Layout

```
dags/               Airflow DAGs (philvolcs.py, import-seeds.py, jaffle_shop.py, exampledag.py)
dbt/jaffle_shop/    dbt example project
dbt/philvolcs/      dbt project: stg_earthquakes → flash_report_input (postgres), flash_reports (bigquery)
tests/dags/         pytest DAG tests (the only tests)
include/            PHIVOLCS GlobalSign intermediate cert
plugins/            empty
```

Secrets live in gitignored `.env` and `airflow_settings.yaml` and never reach the repo.
