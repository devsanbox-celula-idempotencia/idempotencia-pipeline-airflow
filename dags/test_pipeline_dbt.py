from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# El secreto: Exportar el PATH de Python explícitamente en el shell desnudo de Airflow
DBT_CMD = "export PATH=/home/airflow/.local/bin:$PATH && cd /opt/airflow/dbt && dbt --no-use-colors"

default_args = {
    "owner": "simulacro_steven",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="00_TEST_ENSAYO_DBT",
    default_args=default_args,
    description="DAG rápido para ensayar la conexión y modelos dbt de prueba",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ensayo", "dbt", "postgres"],
) as dag:

    # 1. dbt debug
    task_debug = BashOperator(
        task_id="1_dbt_debug",
        bash_command=f"{DBT_CMD} debug",
    )

    # 2. dbt run
    task_run = BashOperator(
        task_id="2_dbt_run",
        bash_command=f"{DBT_CMD} run",
    )

    # 3. dbt test
    task_test = BashOperator(
        task_id="3_dbt_test",
        bash_command=f"{DBT_CMD} test",
    )

    # Flujo lineal directo
    task_debug >> task_run >> task_test