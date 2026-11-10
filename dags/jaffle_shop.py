from airflow.sdk import Asset
from cosmos import DbtDag
from cosmos.config import ExecutionConfig, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import InvocationMode
from cosmos.profiles import PostgresUserPasswordProfileMapping
from pendulum import datetime

dbt_executable_path = "/usr/local/airflow/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="local",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres",
        profile_args={"schema": "public"},
    ),
)

jaffle_shop = DbtDag(
    dag_id="jaffle_shop",
    project_config=ProjectConfig(
        dbt_project_path="/usr/local/airflow/dbt/jaffle_shop",
        project_name="jaffle_shop",
    ),
    profile_config=profile_config,
    # dbt lives only in dbt_venv: RenderConfig drives parse-time `dbt ls`,
    # ExecutionConfig drives task-time runs, operator_args the operators themselves.
    # SUBPROCESS is required: DBT_RUNNER (default) needs dbt importable in the
    # Airflow env and forbids render_config.dbt_executable_path.
    render_config=RenderConfig(
        dbt_executable_path=dbt_executable_path,
        invocation_mode=InvocationMode.SUBPROCESS,
    ),
    execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
    operator_args={"dbt_executable_path": dbt_executable_path},
    start_date=datetime(2023, 1, 1),
    schedule=[Asset("SEED://JAFFLE_SHOP")],
    tags=["dbt"],
    default_args={"retries": 2},
)

jaffle_shop
