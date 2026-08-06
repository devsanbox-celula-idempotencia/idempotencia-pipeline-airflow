"""
DAG: datamart_pipeline
======================
Pipeline ETL/ELT completo para DataMart S.A.S.

Fuentes:
  - data/data.csv          → raw.ecommerce_data   (Kaggle ecommerce-data)
  - data/online_retail_II.csv → raw.online_retail_ii (Kaggle historial)

Destino:
  - PostgreSQL externo, schemas: raw / staging / marts

Tareas:
  1. crear_schemas_y_tablas  → Crea schemas e infraestructura raw en la BD
  2. cargar_ecommerce_raw    → Carga data.csv a raw.ecommerce_data
  3. cargar_online_retail_raw → Carga online_retail_II.csv a raw.online_retail_ii
  4. dbt_run_staging         → Ejecuta modelos de staging (limpieza)
  5. dbt_run_marts           → Ejecuta modelos de marts (Star Schema)
  6. dbt_test                → Valida calidad de datos con dbt test

Idempotencia: cada carga de raw hace TRUNCATE antes de insertar,
y dbt con materialized='table' recrea las tablas en cada ejecución.
"""

from __future__ import annotations

import io
import json
import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ─── Constantes ──────────────────────────────────────────────────────────────

# ID de la Airflow Connection registrada automáticamente en el init container
CONN_ID = "postgres_datamart"

# Directorio del proyecto dbt dentro del contenedor (montado en docker-compose)
DBT_DIR = "/opt/airflow/dbt"

# Prefijo para todos los comandos dbt en BashOperator.
# El 'export PATH' es necesario porque pip instala dbt en ~/.local/bin,
# que no está en el PATH por defecto cuando Airflow ejecuta un bash shell.
DBT_CMD = f"export PATH=/home/airflow/.local/bin:$PATH && dbt --no-use-colors"

# ─── DDL de la capa raw ───────────────────────────────────────────────────────

_DDL_SCHEMAS = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
"""

_DDL_ECOMMERCE = """
CREATE TABLE IF NOT EXISTS raw.ecommerce_data (
    invoice_no   TEXT,
    stock_code   TEXT,
    description  TEXT,
    quantity     TEXT,
    invoice_date TEXT,
    unit_price   TEXT,
    customer_id  TEXT,
    country      TEXT,
    _source      TEXT DEFAULT 'ecommerce_data',
    _loaded_at   TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""

_DDL_ONLINE_RETAIL = """
CREATE TABLE IF NOT EXISTS raw.online_retail_ii (
    invoice      TEXT,
    stock_code   TEXT,
    description  TEXT,
    quantity     TEXT,
    invoice_date TEXT,
    price        TEXT,
    customer_id  TEXT,
    country      TEXT,
    _source      TEXT DEFAULT 'online_retail_ii',
    _loaded_at   TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""

_DDL_REJECTED = """
CREATE TABLE IF NOT EXISTS raw.rejected_records (
    id               SERIAL PRIMARY KEY,
    source_table     TEXT,
    invoice_no       TEXT,
    stock_code       TEXT,
    rejection_reason TEXT,
    raw_row          TEXT,
    rejected_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""


# ─── Funciones de las tareas ─────────────────────────────────────────────────

def crear_schemas_y_tablas(**kwargs):
    """
    Tarea 1: Crea los schemas (raw, staging, marts) y las tablas raw
    si todavía no existen. Es idempotente gracias a 'IF NOT EXISTS'.
    """
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    for ddl in [_DDL_SCHEMAS, _DDL_ECOMMERCE, _DDL_ONLINE_RETAIL, _DDL_REJECTED]:
        cursor.execute(ddl)

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Schemas y tablas raw creados/verificados correctamente.")


def _insertar_rechazados(cursor, source_table, rows_rechazadas):
    """Guarda registros inválidos en raw.rejected_records."""
    if not rows_rechazadas:
        return
    for motivo, row_dict in rows_rechazadas:
        cursor.execute(
            """
            INSERT INTO raw.rejected_records
                (source_table, invoice_no, stock_code, rejection_reason, raw_row)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                source_table,
                row_dict.get("invoice_no") or row_dict.get("invoice"),
                row_dict.get("stock_code"),
                motivo,
                json.dumps(row_dict, default=str),
            ),
        )


def cargar_ecommerce_raw(**kwargs):
    """
    Tarea 2: Carga data.csv → raw.ecommerce_data.

    Idempotencia: hace TRUNCATE antes de insertar. Si el DAG se ejecuta
    dos veces el mismo día, el resultado es exactamente el mismo.

    Registros rechazados (precio=0 en venta, código invalido especial):
    van a raw.rejected_records con el motivo.
    """
    data_path = Variable.get("data_path", default_var="/opt/airflow/data")
    batch_size = int(Variable.get("batch_size", default_var="10000"))
    csv_file = f"{data_path}/data.csv"

    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Idempotencia: limpiar antes de cargar
    cursor.execute("TRUNCATE TABLE raw.ecommerce_data")
    cursor.execute(
        "DELETE FROM raw.rejected_records WHERE source_table = 'ecommerce_data'"
    )

    total_ok = 0
    total_rechazados = 0

    for chunk in pd.read_csv(
        csv_file, encoding="latin-1", dtype=str, chunksize=batch_size
    ):
        chunk = chunk.where(pd.notna(chunk), None)
        rows_ok = []
        rows_rechazadas = []

        for _, row in chunk.iterrows():
            row_dict = {
                "invoice_no":   row.get("InvoiceNo"),
                "stock_code":   row.get("StockCode"),
                "description":  row.get("Description"),
                "quantity":     row.get("Quantity"),
                "invoice_date": row.get("InvoiceDate"),
                "unit_price":   row.get("UnitPrice"),
                "customer_id":  row.get("CustomerID"),
                "country":      row.get("Country"),
            }

            # Regla de rechazo: precio cero o negativo en una venta
            # (facturas C son devoluciones: se cargan pero con precio de referencia)
            invoice = (row_dict["invoice_no"] or "").upper().strip()
            precio_str = row_dict["unit_price"] or ""
            try:
                precio = float(precio_str)
            except ValueError:
                precio = None

            es_devolucion = invoice.startswith("C")

            if precio is not None and precio <= 0 and not es_devolucion:
                rows_rechazadas.append(
                    ("unit_price <= 0 en una venta", row_dict)
                )
                continue

            rows_ok.append(tuple(row_dict.values()))

        if rows_ok:
            # Inserción en bloque (más rápido que fila a fila).
            # Se usa copy_expert (no copy_from) porque copy_from pierde el
            # schema 'raw' cuando el search_path de la sesión es 'public'
            # (caso típico con PgBouncer). copy_expert escribe el schema
            # explícito en el comando COPY, evitando "relation does not exist".
            buf = io.StringIO()
            for r in rows_ok:
                line = "\t".join("" if v is None else str(v) for v in r)
                buf.write(line + "\n")
            buf.seek(0)
            cursor.copy_expert(
                """
                COPY raw.ecommerce_data
                    (invoice_no, stock_code, description, quantity,
                     invoice_date, unit_price, customer_id, country)
                FROM STDIN WITH (FORMAT text)
                """,
                buf,
            )
            total_ok += len(rows_ok)

        _insertar_rechazados(cursor, "ecommerce_data", rows_rechazadas)
        total_rechazados += len(rows_rechazadas)

    conn.commit()
    cursor.close()
    conn.close()
    logging.info(
        f"ecommerce_data: {total_ok} registros cargados, "
        f"{total_rechazados} rechazados."
    )


def cargar_online_retail_raw(**kwargs):
    """
    Tarea 3: Carga online_retail_II.csv → raw.online_retail_ii.
    Misma lógica de idempotencia y rechazo que la tarea anterior.
    """
    data_path = Variable.get("data_path", default_var="/opt/airflow/data")
    batch_size = int(Variable.get("batch_size", default_var="10000"))
    csv_file = f"{data_path}/online_retail_II.csv"

    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE raw.online_retail_ii")
    cursor.execute(
        "DELETE FROM raw.rejected_records WHERE source_table = 'online_retail_ii'"
    )

    total_ok = 0
    total_rechazados = 0

    for chunk in pd.read_csv(
        csv_file, encoding="utf-8", dtype=str, chunksize=batch_size
    ):
        chunk = chunk.where(pd.notna(chunk), None)
        rows_ok = []
        rows_rechazadas = []

        for _, row in chunk.iterrows():
            # "Customer ID" (con espacio) es el nombre real de la columna en Kaggle
            row_dict = {
                "invoice":      row.get("Invoice"),
                "stock_code":   row.get("StockCode"),
                "description":  row.get("Description"),
                "quantity":     row.get("Quantity"),
                "invoice_date": row.get("InvoiceDate"),
                "price":        row.get("Price"),
                "customer_id":  row.get("Customer ID"),
                "country":      row.get("Country"),
            }

            invoice = (row_dict["invoice"] or "").upper().strip()
            precio_str = row_dict["price"] or ""
            try:
                precio = float(precio_str)
            except ValueError:
                precio = None

            es_devolucion = invoice.startswith("C")

            if precio is not None and precio <= 0 and not es_devolucion:
                rows_rechazadas.append(
                    ("price <= 0 en una venta", row_dict)
                )
                continue

            rows_ok.append(tuple(row_dict.values()))

        if rows_ok:
            # Se usa copy_expert (no copy_from) por la misma razón que en
            # cargar_ecommerce_raw: copy_from no resuelve el schema 'raw'
            # bajo PgBouncer con search_path='public'.
            buf = io.StringIO()
            for r in rows_ok:
                line = "\t".join("" if v is None else str(v) for v in r)
                buf.write(line + "\n")
            buf.seek(0)
            cursor.copy_expert(
                """
                COPY raw.online_retail_ii
                    (invoice, stock_code, description, quantity,
                     invoice_date, price, customer_id, country)
                FROM STDIN WITH (FORMAT text)
                """,
                buf,
            )
            total_ok += len(rows_ok)

        _insertar_rechazados(cursor, "online_retail_ii", rows_rechazadas)
        total_rechazados += len(rows_rechazadas)

    conn.commit()
    cursor.close()
    conn.close()
    logging.info(
        f"online_retail_ii: {total_ok} registros cargados, "
        f"{total_rechazados} rechazados."
    )


# ─── Definición del DAG ───────────────────────────────────────────────────────

default_args = {
    "owner": "estudiante",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,                        # Reintentos automáticos ante fallos
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="datamart_pipeline",
    default_args=default_args,
    description="Pipeline ETL/ELT DataMart S.A.S.: raw → staging → marts",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,                       # No ejecutar fechas pasadas al activar
    tags=["datamart", "etl", "dbt", "postgres"],
    doc_md="""
## Pipeline DataMart S.A.S.

Orquesta el flujo completo de datos desde los CSV de Kaggle hasta el
Data Warehouse en PostgreSQL, usando dbt para las transformaciones.

### Flujo de tareas

### Variables de Airflow usadas
- `data_path`: directorio de los CSVs dentro del contenedor
- `batch_size`: filas por lote para la carga raw

### Conexiones de Airflow usadas
- `postgres_datamart`: conexión al PostgreSQL externo
    """,
) as dag:

    # ── Tarea 1 ───────────────────────────────────────────────────────────────
    t1_crear_infra = PythonOperator(
        task_id="crear_schemas_y_tablas",
        python_callable=crear_schemas_y_tablas,
        doc_md="Crea los schemas raw/staging/marts y las tablas raw si no existen.",
    )

    # ── Tareas 2 y 3 (paralelas) ─────────────────────────────────────────────
    t2_cargar_ecommerce = PythonOperator(
        task_id="cargar_ecommerce_raw",
        python_callable=cargar_ecommerce_raw,
        doc_md="Carga data.csv → raw.ecommerce_data. TRUNCATE + INSERT para idempotencia.",
    )

    t3_cargar_online_retail = PythonOperator(
        task_id="cargar_online_retail_raw",
        python_callable=cargar_online_retail_raw,
        doc_md="Carga online_retail_II.csv → raw.online_retail_ii.",
    )

    # ── Tarea 4: dbt staging ─────────────────────────────────────────────────
    t4_dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"dbt run --no-use-colors "
            f"--select stg_ecommerce stg_online_retail stg_transactions_unified "
            f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
        ),
        doc_md="Ejecuta los modelos de staging: limpieza, normalización y unificación.",
    )

    # ── Tarea 5: dbt marts ───────────────────────────────────────────────────
    t5_dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"dbt run --no-use-colors "
            f"--select dim_producto dim_cliente dim_tiempo dim_pais "
            f"fact_ventas fact_devoluciones mart_revenue_producto "
            f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
        ),
        doc_md="Ejecuta el Star Schema: dimensiones, tablas de hechos y mart analítico.",
    )

    # ── Tarea 6: dbt test ────────────────────────────────────────────────────
    t6_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test --no-use-colors "
            f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
        ),
        doc_md="Valida not_null, unique y accepted_values sobre staging y marts.",
    )

    # ── Dependencias explícitas ───────────────────────────────────────────────
    #
    #  t1 ─► t2 ──┐
    #             ├──► t4 ──► t5 ──► t6
    #  t1 ─► t3 ──┘
    #
    t1_crear_infra >> [t2_cargar_ecommerce, t3_cargar_online_retail]
    [t2_cargar_ecommerce, t3_cargar_online_retail] >> t4_dbt_staging
    t4_dbt_staging >> t5_dbt_marts >> t6_dbt_test