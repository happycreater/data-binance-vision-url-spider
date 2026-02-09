import os
from dotenv import load_dotenv
import pandas as pd
from clickhouse_connect import get_client
import zipfile

RAW_DATA_ROOT = "data.binance.vision"


def file_has_header(file) -> bool:
    # 读取文件的第一行
    first_line = file.readline().decode("utf-8").strip()
    # 检查第一行是否包含非数字字符，假设数据行主要由数字组成
    has_header = any(char.isalpha() for char in first_line)
    return has_header


class BinanceUmKlines1m:
    def __init__(self):
        self.pattern = "data/futures/um/daily/klines/SYMBOL/1m/"
        self.table = "klines_1m"
        self.client = get_client(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
            database="binance_um",
        )
        self.endpoint = self.pattern.split("SYMBOL")[0]
        self.root = os.path.join(RAW_DATA_ROOT, self.endpoint)
        self.symbols = os.listdir(self.root)
        self.schema = {
            "open_time": "Int64",
            "open": "Float64",
            "high": "Float64",
            "low": "Float64",
            "close": "Float64",
            "volume": "Float64",
            "close_time": "Int64",
            "quote_asset_volume": "Float64",
            "number_of_trades": "UInt32",
            "taker_buy_base_asset_volume": "Float64",
            "taker_buy_quote_asset_volume": "Float64",
            "ignore": "UInt8",
        }

    def get_unprocessed_zip_files(self, symbol: str):
        symbol_path = os.path.join(RAW_DATA_ROOT, self.pattern).replace(
            "SYMBOL", symbol
        )
        zip_files = os.listdir(symbol_path)
        unprocessed_files = []
        for zip_file in zip_files:
            if zip_file.endswith(".zip"):
                zip_path = os.path.join(symbol_path, zip_file)
                unprocessed_files.append(zip_path)
        return unprocessed_files

    def process_one_zip(self, symbol: str, file: str):
        with zipfile.ZipFile(file, "r") as zip_ref:
            with zip_ref.open(zip_ref.namelist()[0]) as f:
                skip_lines = int(file_has_header(f))
                f.seek(0)  # 重置文件指针到开头
                df = pd.read_csv(
                    f,
                    header=None,
                    skiprows=skip_lines,
                    dtype=self.schema,
                    names=self.schema.keys(),
                )
                df["symbol"] = symbol
                df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
                df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
                df = df[
                    [
                        "symbol",
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
                    ]
                ]
                self.client.insert_df(self.table, df)
                # Mark the file as processed by creating a .done file
                done_file = file + ".done"
                os.rename(file, done_file)

    def run(self):
        for idx, symbol in enumerate(self.symbols, start=1):
            print(f"Processing symbol {idx}/{len(self.symbols)}: {symbol}")
            up_files = self.get_unprocessed_zip_files(symbol)
            for file in up_files:
                self.process_one_zip(symbol, file)


def main():
    load_dotenv()
    processor = BinanceUmKlines1m()
    processor.run()
    return 0


if __name__ == "__main__":
    main()
