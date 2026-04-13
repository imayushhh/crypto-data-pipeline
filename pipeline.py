import os, time, requests, psycopg2, pandas as pd
from datetime import datetime

DB_URL = os.environ["DATABASE_URL"]

# Binance blocks GitHub Actions IPs — using api.binance.vision (Binance's own CDN mirror)
# and a browser User-Agent as a fallback to avoid IP blocking
BINANCE_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def setup_tables():
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crypto_data (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                current_price DOUBLE PRECISION,
                market_cap DOUBLE PRECISION,
                total_volume DOUBLE PRECISION,
                price_change_percentage_24h DOUBLE PRECISION,
                circulating_supply DOUBLE PRECISION,
                total_supply DOUBLE PRECISION,
                date DATE NOT NULL
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orderbook (
                id SERIAL PRIMARY KEY,
                coin VARCHAR(50) NOT NULL,
                level INTEGER,
                bid_price DOUBLE PRECISION,
                bid_quantity DOUBLE PRECISION,
                ask_price DOUBLE PRECISION,
                ask_quantity DOUBLE PRECISION,
                date DATE NOT NULL
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS derivatives_data (
                id SERIAL PRIMARY KEY,
                coin VARCHAR(50) NOT NULL,
                mark_price DOUBLE PRECISION,
                estimated_settle_price DOUBLE PRECISION,
                index_price DOUBLE PRECISION,
                funding_rate DOUBLE PRECISION,
                interest_rate DOUBLE PRECISION,
                date_time TIMESTAMP NOT NULL
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crypto_24market_data (
                id SERIAL PRIMARY KEY,
                coin VARCHAR(50) NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                price_change_percent DOUBLE PRECISION,
                last_price DOUBLE PRECISION,
                volume DOUBLE PRECISION,
                open_price DOUBLE PRECISION,
                high_price DOUBLE PRECISION,
                low_price DOUBLE PRECISION,
                quote_volume DOUBLE PRECISION,
                trade_count INTEGER,
                bid_price DOUBLE PRECISION,
                ask_price DOUBLE PRECISION,
                date DATE NOT NULL
            )
        """)

        conn.commit()
        print("✔ Tables set up successfully.")
    except Exception as e:
        print(f"✖ setup_tables failed: {e}")
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def cleanup_old_data():
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        cursor.execute("DELETE FROM crypto_data WHERE date < CURRENT_DATE - INTERVAL '30 days'")
        print(f"Deleted {cursor.rowcount} rows from crypto_data")

        cursor.execute("DELETE FROM orderbook WHERE date < CURRENT_DATE - INTERVAL '30 days'")
        print(f"Deleted {cursor.rowcount} rows from orderbook")

        cursor.execute("DELETE FROM derivatives_data WHERE date_time < NOW() - INTERVAL '30 days'")
        print(f"Deleted {cursor.rowcount} rows from derivatives_data")

        cursor.execute("DELETE FROM crypto_24market_data WHERE date < CURRENT_DATE - INTERVAL '30 days'")
        print(f"Deleted {cursor.rowcount} rows from crypto_24market_data")

        conn.commit()
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def run_crypto_data():
    conn = None
    cursor = None
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&sparkline=false"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        df = pd.DataFrame(data)
        df = df.reindex(columns=[
            "name", "current_price", "market_cap", "total_volume",
            "price_change_percentage_24h", "circulating_supply", "total_supply",
        ])

        total_supply_values = df["total_supply"].copy()
        df = df.fillna(0)
        df["date"] = datetime.now().date()

        rows = []
        for i, row in enumerate(df.itertuples(index=False, name=None)):
            raw_ts = total_supply_values.iloc[i]
            if raw_ts is None or raw_ts == "" or (isinstance(raw_ts, float) and pd.isna(raw_ts)):
                total_supply = None
            else:
                total_supply = float(raw_ts)

            rows.append((
                row[0],
                float(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(row[5]),
                total_supply,
                row[7],
            ))

        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        if rows:
            cursor.executemany("""
                INSERT INTO crypto_data
                (name, current_price, market_cap, total_volume, price_change_percentage_24h,
                 circulating_supply, total_supply, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, rows)
            conn.commit()
            print(f"✔ crypto_data inserted successfully ({len(rows)} rows)")
        else:
            print("⚠ crypto_data: no rows to insert")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def run_order_book():
    coins = {
        "Bitcoin": "BTCUSDT", "Ethereum": "ETHUSDT", "Binance Coin": "BNBUSDT",
        "Solana": "SOLUSDT", "XRP": "XRPUSDT", "Dogecoin": "DOGEUSDT",
        "Cardano": "ADAUSDT", "Avalanche": "AVAXUSDT", "Toncoin": "TONUSDT",
        "Polkadot": "DOTUSDT", "Polygon": "MATICUSDT", "Litecoin": "LTCUSDT",
        "Chainlink": "LINKUSDT", "TRON": "TRXUSDT", "Cosmos": "ATOMUSDT",
        "NEAR Protocol": "NEARUSDT", "Stellar": "XLMUSDT", "Filecoin": "FILUSDT",
        "Algorand": "ALGOUSDT", "ApeCoin": "APEUSDT"
    }

    rows = []

    for coin_name, symbol in coins.items():
        try:
            url = f"https://api.binance.vision/api/v3/depth?symbol={symbol}&limit=5"
            response = requests.get(url, timeout=30, headers=BINANCE_HEADERS)
            if response.status_code != 200:
                print(f"Error fetching orderbook {coin_name}: {response.status_code}")
                continue

            order_book = response.json()
            bids = order_book.get("bids", [])
            asks = order_book.get("asks", [])

            for i in range(min(len(bids), len(asks), 5)):
                rows.append((
                    coin_name,
                    i + 1,
                    float(bids[i][0]),
                    float(bids[i][1]),
                    float(asks[i][0]),
                    float(asks[i][1]),
                    pd.Timestamp.now().date(),
                ))
        except Exception as e:
            print(f"Exception for orderbook {coin_name}: {e}")
            continue

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        if rows:
            cursor.executemany("""
                INSERT INTO orderbook
                (coin, level, bid_price, bid_quantity, ask_price, ask_quantity, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, rows)
            conn.commit()
            print(f"✔ orderbook inserted successfully ({len(rows)} rows)")
        else:
            print("⚠ orderbook: no rows to insert")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def run_derivatives_data():
    # Switched from Binance fapi (geo-blocked in Canada, HTTP 451)
    # to Bybit public API — no geo restrictions, no API key needed
    # One single API call returns all tickers — faster than per-coin loop
    symbol_to_name = {
        "BTCUSDT": "Bitcoin", "ETHUSDT": "Ethereum", "BNBUSDT": "Binance Coin",
        "SOLUSDT": "Solana", "XRPUSDT": "XRP", "DOGEUSDT": "Dogecoin",
        "ADAUSDT": "Cardano", "AVAXUSDT": "Avalanche", "TONUSDT": "Toncoin",
        "DOTUSDT": "Polkadot", "MATICUSDT": "Polygon", "LTCUSDT": "Litecoin",
        "LINKUSDT": "Chainlink", "TRXUSDT": "TRON", "ATOMUSDT": "Cosmos",
        "NEARUSDT": "NEAR Protocol", "XLMUSDT": "Stellar", "FILUSDT": "Filecoin",
        "ALGOUSDT": "Algorand", "APEUSDT": "ApeCoin"
    }

    rows = []
    try:
        url = "https://api.bybit.com/v5/market/tickers?category=linear"
        response = requests.get(url, timeout=30, headers=BINANCE_HEADERS)
        response.raise_for_status()
        tickers = response.json().get("result", {}).get("list", [])

        for t in tickers:
            symbol = t.get("symbol")
            if symbol not in symbol_to_name:
                continue
            rows.append((
                symbol_to_name[symbol],
                float(t.get("markPrice") or 0),
                float(t.get("prevPrice24h") or 0),  # closest to estimatedSettlePrice
                float(t.get("indexPrice") or 0),
                float(t.get("fundingRate") or 0),
                0.0003,                              # Bybit fixed interest rate
                datetime.now(),
            ))
    except Exception as e:
        print(f"Exception fetching derivatives from Bybit: {e}")

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        if rows:
            cursor.executemany("""
                INSERT INTO derivatives_data
                (coin, mark_price, estimated_settle_price, index_price, funding_rate, interest_rate, date_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, rows)
            conn.commit()
            print(f"✔ derivatives_data inserted successfully ({len(rows)} rows)")
        else:
            print("⚠ derivatives_data: no rows to insert")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def run_ticker_data():
    coins = {
        "Bitcoin": "BTCUSDT", "Ethereum": "ETHUSDT", "Binance Coin": "BNBUSDT",
        "Solana": "SOLUSDT", "XRP": "XRPUSDT", "Dogecoin": "DOGEUSDT",
        "Cardano": "ADAUSDT", "Avalanche": "AVAXUSDT", "Toncoin": "TONUSDT",
        "Polkadot": "DOTUSDT", "Polygon": "MATICUSDT", "Litecoin": "LTCUSDT",
        "Chainlink": "LINKUSDT", "TRON": "TRXUSDT", "Cosmos": "ATOMUSDT",
        "NEAR Protocol": "NEARUSDT", "Stellar": "XLMUSDT", "Filecoin": "FILUSDT",
        "Algorand": "ALGOUSDT", "ApeCoin": "APEUSDT"
    }

    rows = []

    for coin_name, symbol in coins.items():
        url = f"https://api.binance.vision/api/v3/ticker/24hr?symbol={symbol}"
        try:
            response = requests.get(url, timeout=30, headers=BINANCE_HEADERS)
            if response.status_code == 200:
                d = response.json()
                rows.append((
                    coin_name,
                    str(d.get("symbol", symbol)),
                    float(d.get("priceChangePercent") or 0),
                    float(d.get("lastPrice") or 0),
                    float(d.get("volume") or 0),
                    float(d.get("openPrice") or 0),
                    float(d.get("highPrice") or 0),
                    float(d.get("lowPrice") or 0),
                    float(d.get("quoteVolume") or 0),
                    int(d.get("count") or 0),
                    float(d.get("bidPrice") or 0),
                    float(d.get("askPrice") or 0),
                    pd.Timestamp.now().date(),
                ))
            else:
                print(f"Error fetching ticker {symbol}: {response.status_code}")
        except Exception as e:
            print(f"Exception fetching ticker {coin_name}: {e}")

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        if rows:
            cursor.executemany("""
                INSERT INTO crypto_24market_data
                (coin, symbol, price_change_percent, last_price, volume, open_price, high_price,
                 low_price, quote_volume, trade_count, bid_price, ask_price, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, rows)
            conn.commit()
            print(f"✔ crypto_24market_data inserted successfully ({len(rows)} rows)")
        else:
            print("⚠ crypto_24market_data: no rows to insert")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def run_step(step_name, step_function):
    try:
        print(f"\n--- Running {step_name} ---")
        step_function()
        return True
    except Exception as exc:
        print(f"✖ {step_name} failed: {exc}")
        return False


def main():
    print("Starting pipeline...")
    setup_tables()
    cleanup_old_data()

    results = [
        run_step("crypto_data",          run_crypto_data),
        run_step("orderbook",            run_order_book),
        run_step("derivatives_data",     run_derivatives_data),
        run_step("crypto_24market_data", run_ticker_data),
    ]

    if all(results):
        print("\n✔ Pipeline completed successfully.")
    else:
        failed = [["crypto_data", "orderbook", "derivatives_data", "crypto_24market_data"][i]
                  for i, r in enumerate(results) if not r]
        print(f"\n⚠ Pipeline completed with failures: {failed}")
        exit(1)


if __name__ == "__main__":
    main()