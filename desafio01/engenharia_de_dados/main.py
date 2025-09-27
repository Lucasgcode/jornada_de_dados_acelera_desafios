#%%
from dotenv import load_dotenv
import os
import pandas as pd
import requests
import csv
from datetime import datetime
import time

load_dotenv()

API_GET_BITCOIN_PRICE = os.getenv('API_GET_BITCOIN_PRICE')


#%%
def get_raw_bitcoin_price():
    r = requests.get(API_GET_BITCOIN_PRICE)

    if r.status_code == 200:
        return r.json()['data']
    else:
        return False


# Extrair
rawBtcPrice = get_raw_bitcoin_price()

#%%
# Transformar

def transform_btc_price(rawBtcPrice):
    
    return {
        "valor": float(rawBtcPrice["amount"]),
        "criptomoeda": rawBtcPrice["base"],
        "moeda": rawBtcPrice["currency"],
        "timestamp": datetime.now().isoformat()
    }

#%%
# Carregar

# Nome da pasta
def create_folder(btcPrice, path="data/bitcoin"):

    dt = datetime.fromisoformat(btcPrice["timestamp"])
    folder_name = dt.strftime("%Y_%m_%d")

    # Criação da pasta
    path = os.path.join(path, folder_name)
    os.makedirs(path, exist_ok=True)

    return path

# Atualiza/cria o csv

def create_update_csv(btcPrice, path):
    csv_path = os.path.join(path, "btc.csv")

    # Cria ou atualiza o csv
    write_header = not os.path.exists(csv_path)

    with open(csv_path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(btcPrice.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(btcPrice)

    return csv_path

def load_btc(btcPrice):

    path = create_folder(btcPrice)

    return create_update_csv(btcPrice, path)


#%%

# Teste do ETL

while True:
    # Extrair
    rawBtcPrice = get_raw_bitcoin_price()
    print(f"Cotação do Btc: {rawBtcPrice}")

    # Transformar

    btcPrice = transform_btc_price(rawBtcPrice)

    print(f"Transform {btcPrice}")

    # Carregar

    csv_path = load_btc(btcPrice)

    print(f"Preço do bitcoin salvo em: {csv_path}")

    time.sleep(1 * 60)

