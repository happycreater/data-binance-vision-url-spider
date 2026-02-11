#!/usr/bin/env bash
set -euo pipefail

# for quick test
# python gen_url.py --pattern "data/spot/daily/klines/SYMBOL/1m/" --symbol-glob "*ETHUSDT"


# python gen_url.py --pattern "data/spot/daily/klines/SYMBOL/1m/" --symbol-glob "*USDT"
# python gen_url.py --pattern "data/futures/um/daily/klines/SYMBOL/1m/" --symbol-glob "*USDT"
# python gen_url.py --pattern "data/futures/um/monthly/fundingRate/SYMBOL/" --symbol-glob "*USDT"
# python gen_url.py --pattern "data/futures/um/daily/metrics/SYMBOL/" --symbol-glob "*USDT"
# python gen_url.py --pattern "data/futures/um/daily/indexPriceKlines/SYMBOL/1m/" --symbol-glob "*USDT"
# python gen_url.py --pattern "data/futures/um/daily/markPriceKlines/SYMBOL/1m/" --symbol-glob "*USDT"
# python gen_url.py --pattern "data/futures/um/daily/premiumIndexKlines/SYMBOL/1m/" --symbol-glob "*USDT"
python gen_url.py --pattern "data/futures/um/monthly/aggTrades/SYMBOL/" --symbol-glob "*USDT"
# zip files
DATE=$(date +%Y%m%d)
mkdir -p urls
ZIP_PATH=urls/${DATE}.zip
zip -j ${ZIP_PATH} *.txt
rm *.txt