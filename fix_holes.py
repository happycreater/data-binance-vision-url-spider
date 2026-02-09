from clickhouse_connect import get_client
from dotenv import load_dotenv
import os
from binance import Client
import pandas as pd
from binance.enums import HistoricalKlinesType as LT


load_dotenv()
client = Client(requests_params={"proxies": {"https": os.getenv("INTERNET_PROXY")}})
db_client = get_client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    username=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
    database="binance_um",
)
df = db_client.query_df("SELECT * FROM v_klines_1m_clean_gap_info")
print(df)
for idx, row in df.iterrows():
    print(
        f"fixing hole for symbol {row['symbol']} at {row['open_time']}, gap minutes: {row['gap_minutes']}"
    )
    symbol = row["symbol"]
    start_str = str(row["prev_open_time"])
    end_str = str(row["open_time"])
    apires = client.get_historical_klines(
        symbol, "1m", start_str=start_str, end_str=end_str, klines_type=LT.FUTURES
    )
    apires_df = pd.DataFrame(
        apires,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume",
            "ignore",
        ],
    )
    print(f"expected rows: {row['gap_minutes'] - 1}, actual rows: {len(apires_df) - 2}")
    apires_df["symbol"] = symbol
    apires_df["open_time"] = pd.to_datetime(apires_df["open_time"], unit="ms")
    apires_df["close_time"] = pd.to_datetime(apires_df["close_time"], unit="ms")
    apires_df.drop(columns=["ignore"], inplace=True)
    db_client.insert_df("klines_1m", apires_df)
