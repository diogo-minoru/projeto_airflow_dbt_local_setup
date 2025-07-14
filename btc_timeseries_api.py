import requests
import pandas as pd
from dotenv import load_dotenv
import os

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

TWELVE_DATA_KEY: str = os.getenv('TWELVE_DATA_KEY')

# url = f"https://api.twelvedata.com/time_series?apikey={TWELVE_DATA_KEY}&interval=1day&symbol=BTC/USD&country=US&exchange=Binance&format=JSON"

# data = requests.get(url).json()#["values"]
# print(data)
# pd.DataFrame(data).to_json("btc_timeseries.json")