# Imagen base oficial de Airflow. Actualizada a la última versión estable (3.2.2).
FROM apache/airflow:3.2.2

# Paso 1: Instalar dependencias del sistema como root
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Paso 2: Volver al usuario airflow para instalar paquetes Python de forma segura
USER airflow

