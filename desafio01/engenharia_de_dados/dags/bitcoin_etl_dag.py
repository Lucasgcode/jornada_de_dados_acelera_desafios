from __future__ import annotations
import os
import csv
from pathlib import Path
from datetime import datetime
import requests
from dotenv import load_dotenv
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

load_dotenv("/opt/airflow/.env")

API_GET_BITCOIN_PRICE = os.getenv("API_GET_BITCOIN_PRICE")
DATA_DIR = Path("/opt/airflow/data/bitcoin")

def get_raw_bitcoin_price() -> dict:
    r = requests.get(API_GET_BITCOIN_PRICE)

    if r.status_code == 200:
        return r.json()['data']
    raise AirflowFailException(f"Falha ao obter cotação: {r.status_code} - {r.text}")

def transform_btc_price(btcPrice: dict) ->dict:
    ts_iso = pendulum.now("UTC").to_iso8601_string()
    return {
        "valor": float(btcPrice["amount"]),
        "criptomoeda": btcPrice["base"],
        "moeda": btcPrice["currency"],
        "timestamp": ts_iso
    }

def create_partition_folder(ds: str) -> Path:
    folder = DATA_DIR / f"dt={ds}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder

def create_update_csv(record: dict, folder: Path) -> str:
    csv_path = folder / "btc.csv"
    write_header = not csv_path.exists()
    with open(csv_path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(record.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(record)
    return str(csv_path)


# DAG

with DAG(
    dag_id="bitcoin_15min",
    description="ETL do Bitcoin, salvando cotação a cada 15min",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2025, 9, 28, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow", "retries": 1},
    tags=["bitcoin", "etl", "csv"],
) as dag:
    
    def extract(**context):
        data = get_raw_bitcoin_price()
        return data
    
    def transform(**context):
        raw = context["ti"].xcom_pull(task_ids="extract")
        if not raw:
            raise AirflowFailException("Sem dados")
        record = transform_btc_price(raw)

        if record["valor"] <= 0:
            raise AirflowFailException("Valor inválido, menor ou igual a 0.");

        if not record["moeda"]:
            raise AirflowFailException("Moeda ausente.")
        return record
    
    def load(**context):
        ds = context["ds"]  # yyyy-mm-dd (partição)
        record = context["ti"].xcom_pull(task_ids="transform")
        if not record:
            raise AirflowFailException("Sem dados para carregar.")
        folder = create_partition_folder(ds)
        path = create_update_csv(record, folder)
        return path
    
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> transform_task >> load_task