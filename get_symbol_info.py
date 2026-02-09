from binance import Client
from binance.enums import HistoricalKlinesType as LT
import pandas as pd
import os
from clickhouse_connect import get_client
import time
from dotenv import load_dotenv

SYMBOLS = [
    "BTCUSDT"
]

def main():
    load_dotenv()
    client = Client()

    future_exg_info = client.futures_exchange_info()
    symbols = future_exg_info["symbols"]
    for idx, symbol_info in enumerate(symbols, start=1):
        try:
            symbol = symbol_info["symbol"]
            if symbol not in SYMBOLS:
                continue
            print(f"Processing symbol {idx}/{len(symbols)}: {symbol_info['symbol']}")
            base_asset = symbol_info["baseAsset"]
            quote_asset = symbol_info["quoteAsset"]
            listed_at = symbol_info["onboardDate"]
            delisted_at = symbol_info["deliveryDate"]
            status = symbol_info["status"]
            apires = client.get_historical_klines(
                symbol, "1m", start_str="2010-01-01", limit=1, klines_type=LT.FUTURES
            )
            api_kline_start_time = apires[0][0]
            apires = client.get_historical_klines(
                symbol, "1m", end_str="2100-01-01", limit=1, klines_type=LT.FUTURES
            )
            api_kline_end_time = apires[0][0]
            db_client = get_client(
                host=os.getenv("CLICKHOUSE_HOST", "localhost"),
                username=os.getenv("CLICKHOUSE_USER", "default"),
                password=os.getenv("CLICKHOUSE_PASSWORD", ""),
                port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
                database="binance_um",
            )
            df = pd.DataFrame(
                [
                    {
                        "symbol": symbol,
                        "base_asset": base_asset,
                        "quote_asset": quote_asset,
                        "listed_at": listed_at,
                        "delisted_at": delisted_at,
                        "status": status,
                        "api_kline_start_time": api_kline_start_time,
                        "api_kline_end_time": api_kline_end_time,
                        "updated_at": int(time.time() * 1000),
                    }
                ]
            )
            for col in [
                "listed_at",
                "delisted_at",
                "api_kline_start_time",
                "api_kline_end_time",
                "updated_at",
            ]:
                df[col] = pd.to_datetime(df[col], unit="ms")
            db_client.insert_df("symbol_info", df)
        except Exception as e:
            print(f"Error processing symbol {symbol_info['symbol']}: {e}")
            with open("symbol_info_errors.log", "a") as f:
                f.write(f"Error processing symbol {symbol_info['symbol']}: {e}\n")
    return 0


if __name__ == "__main__":
    main()
