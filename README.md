# Pipeline ETL/ELT — DataMart S.A.S.
### Apache Airflow + dbt + PostgreSQL

Pipeline de datos completo que procesa transacciones de comercio electrónico
desde archivos CSV de Kaggle hasta un Data Warehouse en PostgreSQL,
usando **Apache Airflow** (LocalExecutor) como orquestador y **dbt** para transformaciones.

---

## Diagrama del Modelo

![Diagrama del modelo marts](./postgres%20-%20steven_patino%20-%20marts.png)

---

## Estado Actual

El proyecto ya está operativo de punta a punta con esta arquitectura:

- Airflow 3.2.2 orquesta la ingesta y las transformaciones.
- dbt 1.8.4 construye la capa `staging` y el star schema de `marts`.
- PostgreSQL externo almacena `raw`, `staging` y `marts`.
- La capa `staging` limpia, tipa y unifica las dos fuentes CSV.
- La capa `marts` expone dimensiones y hechos con PK/FK físicas en la base.
- La asignación de categorías no depende de la API opcional, sino de reglas locales por palabras clave.
- Las devoluciones se separan de las ventas y el revenue neto se calcula a partir de ambas capas.

---

## Requisitos Previos

| Herramienta | Versión mínima | Verificar con |
|---|---|---|
| Docker Desktop | 4.x | `docker --version` |
| Docker Compose | v2.1+ | `docker compose version` |

> No se requiere Python, Airflow, dbt ni ninguna dependencia local.
> Todo corre dentro de los contenedores Docker.

---

## Estructura del Proyecto

```
Simulacro_PDD_M6/
│
├── Dockerfile              # Airflow 3.2.2 + dbt-core + dbt-postgres
├── requirements.txt        # Versiones fijadas de dependencias
├── docker-compose.yaml     # 4 servicios: postgres-meta, init, webserver, scheduler
│
├── .env.example            # ← PLANTILLA: copiar a .env y editar
├── .env                    # Credenciales reales (excluido de Git)
├── .gitignore
│
├── data/
│   ├── data.csv            # Fuente 1: Kaggle ecommerce-data (muestra incluida)
│   └── online_retail_II.csv # Fuente 2: Kaggle historial (muestra incluida)
│
├── dags/
│   └── etl_pipeline_dag.py # DAG completo del pipeline DataMart
│
├── dbt/
│   ├── profiles.yml        # Conexión BD externa (lee variables de entorno)
│   ├── dbt_project.yml     # Configuración del proyecto dbt
│   ├── macros/
│   │   └── generate_schema_name.sql  # Garantiza schemas raw/staging/marts exactos
│   └── models/
│       ├── staging/
│       │   ├── schema.yml
│       │   ├── stg_ecommerce.sql          # Limpieza de data.csv
│       │   ├── stg_online_retail.sql      # Limpieza de online_retail_II.csv
│       │   └── stg_transactions_unified.sql # Unión deduplicada
│       └── marts/
│           ├── schema.yml
│           ├── dim_producto.sql            # Dimensión productos + categorías
│           ├── dim_cliente.sql             # Dimensión clientes (incluye GUEST)
│           ├── dim_tiempo.sql              # Dimensión tiempo
│           ├── dim_pais.sql                # Dimensión países
│           ├── fact_ventas.sql             # Tabla de hechos: ventas
│           ├── fact_devoluciones.sql       # Tabla de hechos: devoluciones
│           └── mart_revenue_producto.sql   # Mart analítico pre-agregado
│
├── docs/
│   ├── decisiones_tecnicas.md  # Justificación del diseño y casos ambiguos
│   └── queries_negocio.sql     # Consultas SQL para las 7 preguntas de negocio
│
└── logs/                    # Generado automáticamente por Airflow
```

---

## Inicio Rápido

### Paso 0 — Clonar el repositorio

```bash
git clone https://github.com/Steven-Patino/Setup-PDD_M6.git
cd Setup-PDD_M6
```

### Paso 1 — Configurar credenciales

```bash
# Windows PowerShell:
copy .env.example .env

# Mac / Linux:
cp .env.example .env
```

El archivo `.env.example` ya trae los valores correctos para la BD externa del examen.
Si las credenciales cambian, editar la sección `BASE DE DATOS EXTERNA` en `.env`.

> Los datos de muestra en `data/` son suficientes para demostrar el pipeline.
> Para usar los datasets completos de Kaggle, reemplazar los archivos en `data/`
> con los originales descargados de:
> - https://www.kaggle.com/datasets/carrie1/ecommerce-data → `data/data.csv`
> - https://www.kaggle.com/datasets/thedevastator/online-retail-transaction-dataset → `data/online_retail_II.csv`

---

### Paso 2 — Levantar el entorno completo

```bash
docker compose up --build -d
```

Este comando hace **todo automáticamente**:
1. Construye la imagen con Airflow + dbt (~3-5 min la primera vez)
2. Levanta PostgreSQL interno para metadatos de Airflow
3. Migra la base de datos de metadatos
4. Crea el usuario `admin` en la UI
5. **Registra la Airflow Connection `postgres_datamart`** (BD externa)
6. **Inicializa las Airflow Variables** (`data_path`, `batch_size`, `pipeline_env`)
7. Arranca el webserver y el scheduler

Verificar que todos los contenedores estén corriendo:

```bash
docker compose ps
```

Resultado esperado:

```
NAME                    STATUS          PORTS
airflow_postgres_meta   running         5432/tcp
airflow_init            exited (0)      ← Normal: termina tras inicializar
airflow_webserver       running         0.0.0.0:8080->8080/tcp
airflow_scheduler       running
```

---

### Paso 3 — Ejecutar el pipeline

Abrir en el navegador: **http://localhost:8080**

| Campo | Valor |
|---|---|
| Usuario | `admin` |
| Contraseña | `admin` |

1. Buscar el DAG **`datamart_pipeline`**
2. Activarlo con el toggle izquierdo
3. Hacer clic en **▶ Trigger DAG** para ejecutarlo manualmente

El pipeline tarda ~2-5 minutos con los datos de muestra.

### Paso 4 — Validar resultados

1. Confirmar que el DAG `datamart_pipeline` terminó en verde.
2. Revisar que `dbt_test` haya pasado sin errores.
3. Ejecutar la consulta de conteos en `raw`, `staging` y `marts` si quieres validar la carga de extremo a extremo.

---

## Validar que la Connection y Variables quedaron configuradas

Desde la UI de Airflow:
- **Connections:** Admin → Connections → buscar `postgres_datamart`
- **Variables:** Admin → Variables → verificar `data_path`, `batch_size`, `pipeline_env`

Desde la terminal:
```bash
docker exec airflow_scheduler airflow connections get postgres_datamart
docker exec airflow_scheduler airflow variables get data_path
```

---

## Verificar que los datos llegaron al repositorio analítico

Ejecutar la consulta de validación rápida en la BD externa:

```sql
SELECT 'raw.ecommerce_data' AS tabla, COUNT(*) FROM raw.ecommerce_data
UNION ALL
SELECT 'raw.online_retail_ii', COUNT(*) FROM raw.online_retail_ii
UNION ALL
SELECT 'marts.fact_ventas', COUNT(*) FROM marts.fact_ventas
UNION ALL
SELECT 'marts.dim_producto', COUNT(*) FROM marts.dim_producto;
```

---

## Flujo del Pipeline (DAG)

```
crear_schemas_y_tablas
       │
       ├──► cargar_ecommerce_raw     (data.csv → raw.ecommerce_data)
       │
       └──► cargar_online_retail_raw (online_retail_II.csv → raw.online_retail_ii)
                    │ (ambas cargas en paralelo)
                    ▼
            dbt_run_staging
            (stg_ecommerce → stg_online_retail → stg_transactions_unified)
                    │
                    ▼
            dbt_run_marts
            (dim_* → fact_* → mart_revenue_producto)
                    │
                    ▼
            dbt_test
            (not_null, unique, accepted_values)
```

---

## Schemas en la Base de Datos Externa

| Schema | Contiene |
|---|---|
| `raw` | Tablas de ingesta directa desde CSV (texto sin transformar) + `rejected_records` |
| `staging` | Vistas limpias: tipos correctos, fechas UTC, descriptions canónicas, unificación y deduplicación |
| `marts` | Star Schema con PK/FK físicas: `dim_*` + `fact_ventas` + `fact_devoluciones` + `mart_revenue_producto` |

---

## Preguntas de Negocio

Las 7 consultas SQL que responden las preguntas del examen están en
[docs/queries_negocio.sql](docs/queries_negocio.sql).

Ejemplo — Evolución mensual de ventas netas:
```sql
SELECT anio_mes, SUM(revenue_neto) AS ventas_netas
FROM marts.mart_revenue_producto
GROUP BY anio_mes
ORDER BY anio_mes;
```

---

## Comandos de Gestión

```bash
# Ver logs en tiempo real
docker compose logs -f airflow-scheduler

# Apagar (preserva datos)
docker compose down

# Reset completo (borra volúmenes)
docker compose down -v

# Reconstruir imagen (tras cambios en Dockerfile o requirements.txt)
docker compose up --build -d
```

---

## Decisiones Técnicas

Ver [docs/decisiones_tecnicas.md](docs/decisiones_tecnicas.md) para:
- Diseño del modelo estrella y justificación
- Cómo se resolvió cada caso ambiguo (CustomerID nulo, descripciones, duplicados)
- Garantía de idempotencia del DAG
- Estrategia de asignación de categorías sin API

Estado resumido del modelo dbt:
- `staging` deja la data lista para análisis con limpieza y normalización.
- `marts` usa dimensiones y hechos relacionados por claves físicas.
- Las consultas de negocio pueden resolverse desde `mart_revenue_producto` y los facts.

---

## Solución de Problemas

**El webserver tarda en arrancar** → Esperar 30-60 s. Es normal en el primer inicio.

**`airflow_init` termina con código distinto de 0** → Ver logs: `docker compose logs airflow-init`

**dbt no conecta a la BD** → Verificar que las variables `DBT_DB_*` en `.env` son correctas y que el servidor acepta conexiones externas.

**Error de permisos en `logs/` (Linux)** → Ejecutar:
```bash
echo "AIRFLOW_UID=$(id -u)" >> .env && docker compose up -d
```

**Reconstruir sin caché:**
```bash
docker compose build --no-cache && docker compose up -d
```
