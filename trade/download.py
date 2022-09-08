import pandas as pd
import ccxt
import settings as s
from datetime import datetime, timedelta
from ccxt.base.errors import RequestTimeout, BadSymbol
import time
import pytz

binance = ccxt.binance()


def to_timestamp(dt):
    return binance.parse8601(dt.isoformat())


def download_from_binance(symbols: list) -> pd.DataFrame:
    """Download the data from binance for one symbol for the last 10 minutes"""
    records = []
    five_mins_ago = (
        datetime.now(pytz.timezone("Etc/GMT")) - timedelta(minutes=10)
    ).strftime("%Y-%m-%d %H:%M")
    since = to_timestamp(datetime.strptime(five_mins_ago, "%Y-%m-%d %H:%M"))
    symbols = symbols[2:3]
    for symbol in symbols:
        try:
            orders = binance.fetch_trades(symbol + "/BTC", since)
        except RequestTimeout:
            time.sleep(5)
            orders = binance.fetch_trades(symbol + "/BTC", since)
        except BadSymbol:
            print(
                f"Symbol {symbol} doesn't exist in Binance. Continuing with next symbol"
            )
            continue
        for l in orders:
            records.append(
                {
                    "symbol": l["symbol"],
                    "timestamp": l["timestamp"],
                    "datetime": l["datetime"],
                    "side": l["side"],
                    "price": l["price"],
                    "amount": l["amount"],
                    "btc_volume": float(l["price"]) * float(l["amount"]),
                }
            )
    return_df = pd.DataFrame.from_records(records)
    return return_df


if __name__ == "__main__":
    # TODO: add more symbols that might be interesting
    telegram_df = pd.read_csv(s.ROOT_DIR + "/data/pump_telegram.csv")
    symbols = telegram_df["symbol"].unique()
    df = download_from_binance(symbols)
