# dags/philvolcs.py
"""PHIVOLCS latest earthquake -> Postgres -> dbt -> Gemini flash report -> BigQuery.

Manual trigger only. Needs the Airflow connection `bigquery-n8n` and the
variable `GEMINIKEYS` (comma-separated Gemini API keys) from airflow_settings.yaml,
plus the dbt project dbt/philvolcs (targets: postgres for staging, bigquery for
the final flash_reports table in n8n-rogverse.philvolcs).
"""

import logging
import re
import time
from typing import Literal

import requests
from airflow.decorators import task
from airflow.models import DAG, Variable
from cosmos.config import ProfileConfig
from cosmos.operators import DbtRunOperator
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.profiles.bigquery.service_account_keyfile_dict import (
    GoogleCloudServiceAccountDictProfileMapping,
)
from pendulum import datetime, now
from pydantic import BaseModel

PHIVOLCS_URL = "https://earthquake.phivolcs.dost.gov.ph/"
USER_AGENT = "dbt-astro-airflow-starter/1.0 (local research pipeline)"
DBT_EXECUTABLE = "/usr/local/airflow/dbt_venv/bin/dbt"
DBT_PROJECT_DIR = "/usr/local/airflow/dbt/philvolcs"
BQ_PROJECT = "n8n-rogverse"
BQ_DATASET = "philvolcs"
BQ_RAW_TABLE = "raw_llm_flash_reports"
BQ_TABLE_ID = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_RAW_TABLE}"
GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta"
MAX_LLM_ATTEMPTS = 3
# Used only when models.list fails on every key (phsim-astro style fallback).
GEMINI_FALLBACK_MODELS = ["models/gemini-2.5-flash", "models/gemini-2.5-flash-lite"]

log = logging.getLogger(__name__)

DATE_RE = re.compile(r"^\d{1,2} [A-Za-z]+ \d{4} - \d{1,2}:\d{2} [AP]M$")
NUM_RE = re.compile(r"^-?\d+(\.\d+)?$")


class FlashReport(BaseModel):
    """Structured output of the Gemini call (pydantic-ai validates the reply).

    Replaces the hand-rolled FLASH_REPORT_SCHEMA responseSchema dict and
    validate_flash_report(): pydantic-ai sends this schema to the API and
    raises on anything that does not parse into it.
    """

    headline: str
    flash_report_message: str
    magnitude: float
    latitude: float
    longitude: float
    depth_km: int
    location: str
    event_time: str
    urgency: Literal["low", "moderate", "high"]

CREATE_RAW_TABLE_SQL = """
create table if not exists raw_phivolcs_earthquakes (
    event_time_text text not null,
    latitude_text text not null,
    longitude_text text not null,
    depth_text text,
    magnitude_text text,
    location text,
    bulletin_url text,
    scraped_at timestamptz not null default now(),
    primary key (event_time_text, latitude_text, longitude_text)
)
"""

UPSERT_SQL = """
insert into raw_phivolcs_earthquakes
    (event_time_text, latitude_text, longitude_text, depth_text,
     magnitude_text, location, bulletin_url)
values (%s, %s, %s, %s, %s, %s, %s)
on conflict (event_time_text, latitude_text, longitude_text)
do update set
    location = excluded.location,
    bulletin_url = excluded.bulletin_url,
    scraped_at = now()
"""


def parse_bulletin_html(page_html):
    """Parse the PHIVOLCS latest-earthquake table into raw row dicts, newest first.

    Rows are 6-td tables (date, lat, lon, depth, mag, location); the date cell
    links to the bulletin page. Anything that does not match the published
    shapes is skipped - if the page layout changes, zero rows fail the task.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(page_html, "html.parser")
    rows = []
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) != 6:
            continue
        cells = [" ".join(td.stripped_strings) for td in tds]
        if not DATE_RE.match(cells[0]):
            continue
        if not all(NUM_RE.match(c) for c in cells[1:5]):
            continue
        link = tds[0].find("a")
        bulletin_url = None
        if link and link.get("href"):
            bulletin_url = PHIVOLCS_URL + link["href"].replace("\\", "/")
        rows.append(
            {
                "event_time_text": cells[0],
                "latitude_text": cells[1],
                "longitude_text": cells[2],
                "depth_text": cells[3],
                "magnitude_text": cells[4],
                "location": cells[5],
                "bulletin_url": bulletin_url,
            }
        )
    return rows


def pick_flash_models(model_names):
    """Free-tier Gemini flash text models, best first (non-lite before lite)."""
    blocked = ("image", "embedding", "tts", "aqa")
    picked = [
        n
        for n in model_names
        if "gemini" in n and "flash" in n and not any(b in n for b in blocked)
    ]
    return sorted((n for n in picked if "lite" not in n), reverse=True) + sorted(
        (n for n in picked if "lite" in n), reverse=True
    )


def _build_prompt(event):
    return (
        "You are the PHIVOLCS flash report desk. Write a concise, official-style "
        "flash report message for the earthquake below. The message must include "
        "magnitude, depth, latitude, longitude, the location description and the "
        "event time. English, factual, no speculation about damage or casualties. "
        "Also return a one-line headline and an urgency of low, moderate or high "
        "(magnitude 5.0+ = high, 3.0-4.9 = moderate, below 3.0 = low).\n\n"
        f"Event time (UTC): {event['event_time']}\n"
        f"Latitude: {event['latitude']}\n"
        f"Longitude: {event['longitude']}\n"
        f"Depth (km): {event['depth_km']}\n"
        f"Magnitude: {event['magnitude']}\n"
        f"Location: {event['location']}\n"
        f"PHIVOLCS bulletin: {event['bulletin_url'] or 'n/a'}\n\n"
        "Return only the structured JSON object."
    )


def _list_models(keys):
    """Model names that support generateContent, trying each key in turn.

    Returns [] when no key can list - the caller falls back to
    GEMINI_FALLBACK_MODELS (phsim-astro behavior).
    """
    for key in keys:
        try:
            resp = requests.get(
                f"{GEMINI_BASE}/models",
                params={"key": key, "pageSize": 200},
                timeout=30,
            )
        except requests.exceptions.RequestException:
            continue
        if resp.ok:
            try:
                data = resp.json()
            except ValueError:
                continue
            return [
                m["name"]
                for m in data.get("models", [])
                if "generateContent" in m.get("supportedGenerationMethods", [])
            ]
    return []


def _scrub(text):
    """Strip the API key from anything that may reach the logs (requests embeds
    the query string in connection errors)."""
    return re.sub(r"[?&]key=[^&\s]+", "?key=<redacted>", str(text))


def _call_model(key, model, prompt):
    """One structured-output Gemini call through pydantic-ai; raises on failure.

    No inner HTTP retries: google-genai's retry options default to off, so
    the rotation policy in _attempt_report stays the only retry logic.
    """
    from pydantic_ai import Agent
    from pydantic_ai.models.google import GoogleModel
    from pydantic_ai.providers.google import GoogleProvider

    agent = Agent(
        GoogleModel(model, provider=GoogleProvider(api_key=key)),
        output_type=FlashReport,
        model_settings={"timeout": 60},
    )
    return agent.run_sync(prompt).output


def _classify(exc):
    """Rotation bucket for a _call_model failure: one of
    "timeout" | "dead" | "strike" | "capacity" | "burn".

    dead/strike/capacity/timeout switch models without burning an attempt;
    "burn" consumes one of MAX_LLM_ATTEMPTS. Matches pydantic-ai's exception
    taxonomy (ModelNotFoundError=404, ModelRateLimitError=429,
    ModelUnavailableError=503, ModelTimeoutError) with status-code and
    message fallbacks for anything the SDK wraps differently.
    """
    name = type(exc).__name__
    status = getattr(exc, "status_code", None) or getattr(exc, "code", None)
    if "Timeout" in name or isinstance(exc, TimeoutError):
        return "timeout"
    if "NotFound" in name or status == 404:
        return "dead"
    if "RateLimit" in name or status == 429:
        return "strike"
    if (
        "Unavailable" in name
        or "Overloaded" in name
        or status in (502, 503)
        or re.search(r"high demand|overload", _scrub(exc), re.I)
    ):
        return "capacity"
    return "burn"


def _attempt_report(prompt, keys, models):
    """Call Gemini with rotation + retry until one valid flash report comes back.

    Ported from rpfilomeno/phsim-astro scripts/generate-site-data.ts
    (attemptLlm): MAX_LLM_ATTEMPTS counts real failures only - HTTP 404/429,
    capacity spikes (502/503 or "high demand"/"overloaded" in the body) and the
    first hang switch models without burning an attempt; a timeout marks the
    model unresponsive for the rest of the call (never a second hang on the
    same model); 429s strike keys (2 strikes = key dead) and models (429s from
    2 distinct keys = model dead). Keys are logged by index only.
    Shortcut: dead/skip state is per call, not per run - the DAG makes exactly
    one call per run.
    Returns the validated payload with _model set.
    """
    dead_models = {}
    dead_keys = set()
    key_429_strikes = {}
    model_429_keys = {}
    skip = set()  # this call only: capacity / unattributed 429
    wait_next = False  # a burned failure just happened -> micro-backoff first
    refund_hung = False  # only the first hang in a call is free
    last_error = ""
    tries = 0
    i = 0
    max_i = max(len(models), 1) * (MAX_LLM_ATTEMPTS + 1)
    model_cursor = key_cursor = 0
    while i < max_i and tries < MAX_LLM_ATTEMPTS:
        i += 1
        if wait_next:
            wait_next = False
            time.sleep(min(0.5 * 2 ** max(tries - 1, 0), 2.0))
        # round-robin pick: first model that is not dead and not skipped
        model = ""
        for _ in models:
            cand = models[model_cursor % len(models)]
            model_cursor += 1
            if cand in dead_models or cand in skip:
                continue
            model = cand
            break
        if not model:
            continue  # all dead/skipped -> spin out on the i bound below
        key = ""
        for _ in keys:
            cand = keys[key_cursor % len(keys)]
            key_cursor += 1
            if cand in dead_keys:
                continue
            key = cand
            break
        if not key:
            raise RuntimeError("gemini: every key reached 2 strikes; keys exhausted")
        tries += 1
        who = f"try {tries}/{MAX_LLM_ATTEMPTS} {model} key {keys.index(key) + 1}/{len(keys)}"
        try:
            result = _call_model(key, model, prompt)
        except Exception as exc:
            bucket = _classify(exc)
            last_error = f"{model}: {_scrub(exc)}"
            if bucket == "timeout":
                dead_models[model] = "timeout"
                if not refund_hung:
                    refund_hung = True
                    tries -= 1
                log.warning("[llm] %s -> timeout; model marked unresponsive", who)
            elif bucket == "dead":
                dead_models[model] = "HTTP 404"
                tries -= 1
                log.warning("[llm] %s -> %s; model retired", who, last_error)
            elif bucket == "strike":
                # Attribute before banning: strike the key, count distinct keys
                # on the model; kill each only once the evidence is there.
                tries -= 1
                key_429_strikes[key] = key_429_strikes.get(key, 0) + 1
                seen = model_429_keys.setdefault(model, set())
                seen.add(key)
                skip.add(model)
                if key_429_strikes[key] >= 2:
                    dead_keys.add(key)
                if len(seen) >= 2 or len(keys) == 1:
                    dead_models[model] = "HTTP 429"
                log.warning("[llm] %s -> rate-limited; strikes recorded", who)
            elif bucket == "capacity":
                tries -= 1  # capacity spike: switch now, keep the try
                skip.add(model)
                log.warning("[llm] %s -> %s; switching model", who, last_error)
            else:  # burn: real failure, incl. output that failed validation
                wait_next = True
                log.warning("[llm] %s -> %s; retrying", who, last_error)
            continue
        log.info("[llm] ok (%s)", who)
        payload = result.model_dump()
        payload["_model"] = model
        return payload
    if not last_error:
        last_error = "all models dead: " + ", ".join(
            f"{m} ({r})" for m, r in dead_models.items()
        )
    raise RuntimeError(f"gemini failed after {tries} attempts: {last_error}")


def _bigquery_client():
    import json as _json

    from airflow.hooks.base import BaseHook
    from google.cloud import bigquery

    extras = BaseHook.get_connection("bigquery-n8n").extra_dejson
    keyfile = extras.get("keyfile_dict")
    if isinstance(keyfile, str):
        keyfile = _json.loads(keyfile)
    if not keyfile:
        raise ValueError("bigquery-n8n connection has no keyfile_dict extra")
    return bigquery.Client.from_service_account_info(
        keyfile, project=extras.get("project", BQ_PROJECT)
    )


def _ensure_tables(client):
    from google.cloud import bigquery

    client.create_dataset(
        bigquery.Dataset(f"{BQ_PROJECT}.{BQ_DATASET}"), exists_ok=True
    )
    schema = [
        bigquery.SchemaField("event_time", "TIMESTAMP"),
        bigquery.SchemaField("latitude", "FLOAT64"),
        bigquery.SchemaField("longitude", "FLOAT64"),
        bigquery.SchemaField("depth_km", "INT64"),
        bigquery.SchemaField("magnitude", "FLOAT64"),
        bigquery.SchemaField("location", "STRING"),
        bigquery.SchemaField("headline", "STRING"),
        bigquery.SchemaField("flash_report_message", "STRING"),
        bigquery.SchemaField("urgency", "STRING"),
        bigquery.SchemaField("source_model", "STRING"),
        bigquery.SchemaField("generated_at", "TIMESTAMP"),
    ]
    client.create_table(
        bigquery.Table(BQ_TABLE_ID, schema=schema), exists_ok=True
    )


def _already_reported(client, event):
    """True when this event (time + coords) already has a report in BigQuery.

    event['event_time'] is an ISO string by the time this runs; compare it as
    'YYYY-MM-DD HH:MM:SS' against BigQuery's datetime.
    """
    target = event["event_time"][:19].replace("T", " ")
    rows = client.query(
        f"select event_time, latitude, longitude from `{BQ_TABLE_ID}` "
        "order by generated_at desc limit 500"
    ).result()
    for row in rows:
        if (
            row.event_time.strftime("%Y-%m-%d %H:%M:%S") == target
            and round(row.latitude, 4) == round(float(event["latitude"]), 4)
            and round(row.longitude, 4) == round(float(event["longitude"]), 4)
        ):
            return True
    return False


PHIVOLCS_EXTRA_CA = "/usr/local/airflow/include/gsrsaovsslca2018.pem"


def _ca_bundle():
    """certifi + the GlobalSign intermediate PHIVOLCS forgets to serve.

    The PHIVOLCS web server sends only its leaf certificate, so stock Python
    (unlike browsers, which fetch the missing intermediate via AIA) fails
    verification. shortcut: bundle lives in include/, delete once the server
    serves a full chain.
    """
    import certifi

    with open(certifi.where(), "rb") as base, open(PHIVOLCS_EXTRA_CA, "rb") as extra:
        bundle = base.read() + extra.read()
    path = "/tmp/phivolcs-ca-bundle.pem"
    with open(path, "wb") as fh:
        fh.write(bundle)
    return path


@task
def extract_earthquakes():
    """Scrape the PHIVOLCS bulletin table and upsert it into Postgres."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    resp = requests.get(
        PHIVOLCS_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=30,
        verify=_ca_bundle(),
    )
    resp.raise_for_status()
    rows = parse_bulletin_html(resp.text)
    if not rows:
        raise ValueError("PHIVOLCS page parsed to zero earthquake rows")

    hook = PostgresHook(postgres_conn_id="postgres")
    hook.run(CREATE_RAW_TABLE_SQL)
    for row in rows:
        hook.run(
            UPSERT_SQL,
            parameters=(
                row["event_time_text"],
                row["latitude_text"],
                row["longitude_text"],
                row["depth_text"],
                row["magnitude_text"],
                row["location"],
                row["bulletin_url"],
            ),
        )
    return {"rows_upserted": len(rows), "latest": rows[0]["event_time_text"]}


@task
def generate_flash_report():
    """Turn the latest dbt event into a structured Gemini flash report in BigQuery.

    Event fields written to BigQuery come from PHIVOLCS (via dbt), never from
    the model - the LLM only contributes headline, message and urgency. Skips
    events that already have a report; rotates GEMINIKEYS keys and flash models
    on any failure.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    row = PostgresHook(postgres_conn_id="postgres").get_first(
        "select event_time, latitude, longitude, depth_km, magnitude, "
        "location, bulletin_url from flash_report_input limit 1"
    )
    if not row:
        raise ValueError("flash_report_input is empty - did dbt_staging run?")
    event = dict(
        zip(
            (
                "event_time",
                "latitude",
                "longitude",
                "depth_km",
                "magnitude",
                "location",
                "bulletin_url",
            ),
            row,
        )
    )
    event["event_time"] = event["event_time"].isoformat()

    client = _bigquery_client()
    _ensure_tables(client)
    if _already_reported(client, event):
        return {
            "skipped": True,
            "reason": "event already reported",
            "event_time": event["event_time"],
        }

    keys = [
        k.strip()
        for k in Variable.get("GEMINIKEYS", default_var="").split(",")
        if k.strip()
    ]
    if not keys:
        raise ValueError("Airflow Variable GEMINIKEYS is empty")
    models = pick_flash_models(_list_models(keys)) or GEMINI_FALLBACK_MODELS

    prompt = _build_prompt(event)
    result = _attempt_report(prompt, keys, models)

    record = {
        "event_time": event["event_time"],
        "latitude": float(event["latitude"]),
        "longitude": float(event["longitude"]),
        "depth_km": int(event["depth_km"]),
        "magnitude": float(event["magnitude"]),
        "location": event["location"],
        "headline": result["headline"],
        "flash_report_message": result["flash_report_message"],
        "urgency": result["urgency"],
        "source_model": result["_model"],
        "generated_at": now("UTC").isoformat(),
    }
    # Load job, not insert_rows_json: streaming inserts are blocked on the
    # free tier (403 "Streaming insert is not allowed in the free tier").
    job = client.load_table_from_json([record], BQ_TABLE_ID)
    job.result()
    if job.errors:
        raise RuntimeError(f"BigQuery load into {BQ_TABLE_ID} failed: {job.errors}")
    return {
        "skipped": False,
        "model": result["_model"],
        "headline": result["headline"],
        "urgency": result["urgency"],
        "event_time": event["event_time"],
    }


pg_profile = ProfileConfig(
    profile_name="philvolcs",
    target_name="local",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres",
        profile_args={"schema": "public"},
    ),
)

bq_profile = ProfileConfig(
    profile_name="philvolcs",
    target_name="bigquery",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id="bigquery-n8n",
    ),
)

with DAG(
    dag_id="philvolcs",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2},
    tags=["philvolcs", "earthquake", "llm", "bigquery"],
) as dag:

    extract = extract_earthquakes()

    dbt_staging = DbtRunOperator(
        task_id="dbt_staging",
        select=["stg_earthquakes+"],
        project_dir=DBT_PROJECT_DIR,
        dbt_executable_path=DBT_EXECUTABLE,
        profile_config=pg_profile,
    )

    flash_report = generate_flash_report()

    dbt_flash_reports = DbtRunOperator(
        task_id="dbt_flash_reports",
        select=["flash_reports"],
        project_dir=DBT_PROJECT_DIR,
        dbt_executable_path=DBT_EXECUTABLE,
        profile_config=bq_profile,
    )

    extract >> dbt_staging >> flash_report >> dbt_flash_reports

dag
