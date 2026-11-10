# AGENTS.md

Astro (Astronomer) Airflow project wrapping a dbt project (`dbt/jaffle_shop`, the jaffle_shop example). Still mostly scaffold: one example DAG, no dbt-to-Airflow wiring yet. Airflow runs in Docker via the Astro CLI; the README is a one-line stub — trust config and code instead.

## Commands

- Astro CLI is managed by **mise** (`mise.toml` → `astro = "latest"`); use `astro`, not a global install.
- `astro dev start` / `astro dev stop` / `astro dev kill` — local Airflow environment (Docker required). Health check waits 1m by default; use `astro dev start --wait 5m` if the first build is slow.
- `astro dev parse` — DAG parse validation. Runs `.astro/test_dag_integrity_default.py`, which is marked **"DO NOT EDIT"**.
- `astro dev pytest [path]` — run tests. Host Python has no Airflow (`import airflow` fails), so bare `pytest` cannot work. Single file: `astro dev pytest tests/dags/test_dag_example.py`. Extra pytest flags go in `--args "..."` (quoted, e.g. `-k name`). The default env file is `.env`.
- `astro dev logs` / `ps` / `restart` / `bash` — inspect or restart the running environment.
- **No linter, formatter, typechecker, pre-commit, or CI workflow exists.** Verification = `astro dev parse` + `astro dev pytest`. Don't invent extra gates.
- `astro deploy` won't work as-is: `.astro/config.yaml` has only a project name, no deployment configured.

## Enforced DAG conventions (enforced by `tests/dags/test_dag_example.py`)

Every DAG must import cleanly, have non-empty `tags=[...]`, and set `default_args["retries"] >= 2` (the test asserts `>= 2`; the example uses 3). New DAGs missing either fail `astro dev pytest`.

## Layout

- `dags/` — Airflow DAGs. Airflow 3 style: `from airflow.sdk import dag, task, Asset` (Astro Runtime `3.3-12`). Only `exampledag.py` exists.
- `dbt/jaffle_shop/` — dbt project: models, seeds, macros. `dbt_project.yml` expects a profile named `jaffle_shop`.
- `tests/dags/` — pytest DAG tests (the only tests; no dbt tests directory exists yet).
- `plugins/`, `include/` — empty.

## Gotchas

- **`dbt/jaffle_shop` is a broken gitlink**: the index records it as a submodule (mode `160000`), but there is no `.gitmodules` and no `.git` inside. Result: `git status` never shows changes under `dbt/jaffle_shop`, and dbt edits are silently never committed. Before committing dbt changes: `git rm --cached dbt/jaffle_shop`, then add the real files — confirm with the user first.
- **Two requirements files, different destinations**: `requirements.txt` → main Airflow image env (pinned `astronomer-cosmos==1.16.0`); `dbt-requirements.txt` → separate venv at `/usr/local/airflow/dbt_venv` built by the Dockerfile to avoid dependency conflicts (currently unpinned `dbt-core`/`dbt-postgres`, resolved to 1.12.6/1.11.0). `packages.txt` = OS packages. Edit the right file. Python deps install via `uv pip install -c /etc/pip-constraints.txt` (script `/usr/local/bin/install-python-dependencies`), so Airflow itself can never be pinned from requirements.txt.
- **Never add the `[dbt.all]` extra** to `astronomer-cosmos`: on Runtime 3.3 the only resolvable version with it is ≤1.10.3, and those versions crash-loop every Airflow service at startup (`cannot import name 'conf' from partially initialized module 'airflow.configuration'` — circular import fixed in cosmos ≥1.14). Cosmos ≥1.14 with `dbt.all` doesn't resolve at all: dbt-mysql vs dbt-starrocks forces `jinja2==3.1.2` against Airflow's `>=3.1.5`. Verified with `uv pip compile`, 2026-11-10. dbt adapters in the Airflow env are unnecessary — dbt runs from `dbt_venv`.
- **`docker-compose.override.yml` in the project root is auto-merged by `astro dev start`** (used here to mount `./dbt` into containers). Runtime 3.x service names are `api-server`, `scheduler`, `dag-processor`, `triggerer` — there is no `webserver` (Runtime 2 name); a service entry that doesn't match an existing one fails the whole start with "has neither an image nor a build context". The `version:` key is obsolete — omit it. Note Astro already bind-mounts `dags/`, `include/`, `plugins/`, `tests/` automatically; `dbt/` is not among them, hence the override.
- **No dbt profile anywhere** (no `profiles.yml`) and no Cosmos DAG yet — nothing connects Airflow to dbt today. The first Cosmos task group must define the profile itself (Cosmos `ProfileConfig`) and point at the venv dbt executable `/usr/local/airflow/dbt_venv/bin/dbt` rather than the system one.
- **`.env` and `airflow_settings.yaml` are gitignored** — local-only, and `airflow_settings.yaml` is still an empty template (connections/pools/variables defined there never reach the repo).
- `.astro/test_dag_integrity_default.py` is generated/used by `astro dev parse`; never edit it.
