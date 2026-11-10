from airflow import DAG
from airflow.sdk import Asset, TaskGroup
from pendulum import datetime

from cosmos.config import ProfileConfig
from cosmos.operators import DbtRunOperationOperator, DbtSeedOperator
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Local operators require a ProfileConfig; the Airflow connection "postgres"
# must exist (see airflow_settings.yaml) and points at the local Compose DB.
profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="local",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres",
        profile_args={"schema": "public"},
    ),
)

with DAG(
    dag_id="import-seeds",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2},
    tags=["seeds"],
) as dag:

    project_seeds = [
        {
            "project": "jaffle_shop",
            "seeds": ["raw_customers", "raw_payments", "raw_orders"],
        }
    ]

    with TaskGroup(group_id="drop_seeds_if_exist") as drop_seeds:
        for project in project_seeds:
            for seed in project["seeds"]:
                DbtRunOperationOperator(
                    task_id=f"drop_{seed}_if_exists",
                    macro_name="drop_table",
                    args={"table_name": seed},
                    project_dir=f"/usr/local/airflow/dbt/{project['project']}",
                    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
                    profile_config=profile_config,
                )

    create_seeds = DbtSeedOperator(
        task_id="jaffle_shop_seed",
        project_dir="/usr/local/airflow/dbt/jaffle_shop",
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
        profile_config=profile_config,
        outlets=[Asset("SEED://JAFFLE_SHOP")],
    )

    drop_seeds >> create_seeds
