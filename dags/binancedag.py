import os
import json
import time
import requests
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from datetime import datetime, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
# (El HttpOperator ya no se usa para histórico paginado)
# from airflow.providers.http.operators.http import HttpOperator

# ---- RUTAS Y ENDPOINTS ----
BASE_DIR = "/usr/local/airflow/include"
OUT_CSV = f"{BASE_DIR}/criptos_final.csv"

# (Con iteración no usamos estos ENDPOINT_* fijos, pero los dejo por si querés testear 1000 velas)
ENDPOINT_BTC = "/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=1000"
ENDPOINT_SOL = "/api/v3/klines?symbol=SOLUSDT&interval=1h&limit=1000"
ENDPOINT_ETH = "/api/v3/klines?symbol=ETHUSDT&interval=1h&limit=1000"

BTC_DATA_PATH = f"{BASE_DIR}/btc_data.json"
ETH_DATA_PATH = f"{BASE_DIR}/eth_data.json"
SOL_DATA_PATH = f"{BASE_DIR}/sol_data.json"

default_args = {
    "owner": "grupo 2",
    "start_date": datetime.today() - timedelta(days=1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "depends_on_past": False,
}

# ------------------ Utils / Fetch histórico 1 año ------------------

DAYS_BACK = 365
INTERVAL_BINANCE = "1h"

def ensure_dirs():
    Path(BASE_DIR).mkdir(parents=True, exist_ok=True)

def ms_now_utc():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def fetch_full_year(symbol: str, out_path: str, interval: str = INTERVAL_BINANCE, days_back: int = DAYS_BACK):
    """
    Itera contra /api/v3/klines trayendo ~1 año hacia atrás en páginas de 1000 velas.
    Escribe un único JSON por símbolo con todas las velas.
    """
    ensure_dirs()
    end_ms = ms_now_utc()
    start_ms = end_ms - days_back * 24 * 3600 * 1000

    all_rows = []
    cur = start_ms
    total_requests = 0

    while cur < end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": 1000,
            "startTime": cur,
            "endTime": end_ms
        }
        resp = requests.get("https://api.binance.com/api/v3/klines", params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        total_requests += 1

        if not data:
            print(f"[{symbol}] no devolvió más filas (cur={cur})")
            break

        all_rows.extend(data)

        last_close = data[-1][6]   # close_time ms
        cur = last_close + 1       # avanzar 1 ms para no repetir

        print(f"[{symbol}] batch #{total_requests}: filas={len(data)} acumuladas={len(all_rows)} next_cur={cur}")
        # Si vino menos de 1000, ya no hay más páginas dentro del rango
        if len(data) < 1000:
            break

        # anti-rate-limit soft
        time.sleep(0.15)

    # guardado único por símbolo
    with open(out_path, "w") as f:
        json.dump(all_rows, f)

    if all_rows:
        print(f"[{symbol}] total filas guardadas: {len(all_rows)}")
        print(f"[{symbol}] primera vela (open_time): {all_rows[0][0]} | última: {all_rows[-1][0]}")
    else:
        print(f"[{symbol}] WARN: no se guardaron filas")

# ------------------ MERGE & TRANSFORM (tu lógica) ------------------

COLS_RAW = [
    "open_time_ms","open","high","low","close","volume",
    "close_time_ms","quote_volume","trades","taker_base","taker_quote","ignore"
]

COL_ORDER = [
    "symbol","open_time_utc","open_time_ms","open","high","low","close","volume",
    "close_time_utc","close_time_ms","quote_volume","trades","taker_base","taker_quote"
]

NUM_COLS_FLOAT = ["open","high","low","close","volume","quote_volume","taker_base","taker_quote"]

def load_and_normalize_one(path: str, symbol: str) -> pd.DataFrame:
    with open(path, "r") as f:
        arr = json.load(f)
    if not arr or not isinstance(arr, list) or len(arr[0]) != 12:
        raise ValueError(f"Formato inesperado en {path}")
    df = pd.DataFrame(arr, columns=COLS_RAW)
    df["symbol"] = symbol
    df["open_time_utc"]  = pd.to_datetime(df["open_time_ms"],  unit="ms", utc=True)
    df["close_time_utc"] = pd.to_datetime(df["close_time_ms"], unit="ms", utc=True)
    for c in NUM_COLS_FLOAT:
        df[c] = df[c].astype(float)
    df["trades"] = df["trades"].astype(int)
    df = df[COL_ORDER]
    return df

def load_three() -> pd.DataFrame:
    df_btc = load_and_normalize_one(BTC_DATA_PATH, "BTCUSDT")
    df_eth = load_and_normalize_one(ETH_DATA_PATH, "ETHUSDT")
    df_sol = load_and_normalize_one(SOL_DATA_PATH, "SOLUSDT")
    df_all = pd.concat([df_btc, df_eth, df_sol], ignore_index=True)
    return df_all

def tidy_and_check(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["open_time_utc","symbol"]).reset_index(drop=True)
    before = len(df)
    df = df.drop_duplicates(subset=["symbol","open_time_ms"], keep="last")
    dups = before - len(df)
    gaps = (
        df.sort_values(["symbol","open_time_ms"])
          .groupby("symbol")["open_time_ms"]
          .diff()
          .sub(3_600_000)  # 1h en ms
          .ne(0)
          .groupby(df["symbol"])
          .sum()
          .to_dict()
    )
    print(f"[merge] filas: {len(df)} | dups removidos: {dups} | gaps por símbolo: {gaps}")
    return df

def export_csv(df: pd.DataFrame, out_path: str):
    Path(os.path.dirname(out_path)).mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"[merge] escrito: {out_path}")

def merge_transform():
    df_all = load_three()
    df_all = tidy_and_check(df_all)
    export_csv(df_all, OUT_CSV)

# ------------------ DAG ------------------
with DAG(
    dag_id="criptos_etl",
    description="Conectar con API y ver valores (1 año, 1h, 3 símbolos)",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["cripto", "etl"],
) as dag:

    # Fetch histórico 1 año para cada símbolo
    fetch_year_btc = PythonOperator(
        task_id="fetch_year_btc",
        python_callable=fetch_full_year,
        op_kwargs={"symbol": "BTCUSDT", "out_path": BTC_DATA_PATH},
    )

    fetch_year_eth = PythonOperator(
        task_id="fetch_year_eth",
        python_callable=fetch_full_year,
        op_kwargs={"symbol": "ETHUSDT", "out_path": ETH_DATA_PATH},
    )

    fetch_year_sol = PythonOperator(
        task_id="fetch_year_sol",
        python_callable=fetch_full_year,
        op_kwargs={"symbol": "SOLUSDT", "out_path": SOL_DATA_PATH},
    )

    merge_and_transform = PythonOperator(
        task_id="merge_and_transform",
        python_callable=merge_transform,
    )

    # Dependencias
    [fetch_year_btc, fetch_year_eth, fetch_year_sol] >> merge_and_transform
