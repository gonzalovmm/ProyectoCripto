# Proyecto Cripto - ETL con Airflow

Este proyecto forma parte de la materia **Ciencia de Datos** (UTN).  
La idea principal es armar un flujo ETL en **Apache Airflow** que:

1. Se conecta a la API pública de **Binance**.
2. Descarga velas (OHLCV) de **BTC**, **ETH** y **SOL** con intervalo de 1 hora.
3. Itera las llamadas para obtener ~1 año de histórico.
4. Guarda los datos crudos en formato JSON (uno por moneda).
5. Unifica y transforma esos datos en un CSV final con la columna `symbol`.

---

## Tecnologías

- Python 3
- Apache Airflow
- Pandas
- Requests
- Git/GitHub

---

## Cómo correrlo

1. Clonar el repo:
   ```bash
   git clone git@github.com:gonzalovmm/ProyectoCripto.git
   cd ProyectoCripto
2. Levantar Airflow (ejemplo con Astronomer o Docker).
3. Ejecutar el DAG criptos_etl.
4. Revisar en la carpeta data/ el CSV final con las tres monedas.

---

## Autor 

Grupo 2 - UTN-FRM,2025
