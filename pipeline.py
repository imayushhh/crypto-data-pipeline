import os, time, requests, psycopg2, pandas as pd
from datetime import datetime

DB_URL = os.environ["DATABASE_URL"]


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
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def cleanup_old_data():
    conn = None
    cursor = None
    crypto_deleted = 0
    orderbook_deleted = 0
    derivatives_deleted = 0
    market_deleted = 0

    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        cursor.execute("DELETE FROM crypto_data WHERE date < CURRENT_DATE - INTERVAL '30 days'")
        crypto_deleted = cursor.rowcount

        cursor.execute("DELETE FROM orderbook WHERE date < CURRENT_DATE - INTERVAL '30 days'")
        orderbook_deleted = cursor.rowcount

        cursor.execute("DELETE FROM derivatives_data WHERE date_time < NOW() - INTERVAL '30 days'")
        derivatives_deleted = cursor.rowcount

        cursor.execute("DELETE FROM crypto_24market_data WHERE date < CURRENT_DATE - INTERVAL '30 days'")
        market_deleted = cursor.rowcount

        conn.commit()

        print(f"Deleted {crypto_deleted} rows from crypto_data")
        print(f"Deleted {orderbook_deleted} rows from orderbook")
        print(f"Deleted {derivatives_deleted} rows from derivatives_data")
        print(f"Deleted {market_deleted} rows from crypto_24market_data")
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
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        df = pd.DataFrame(data)
        df = df.reindex(columns=[
            "name",
            "current_price",
            "market_cap",
            "total_volume",
            "price_change_percentage_24h",
            "circulating_supply",
            "total_supply",
        ])

        total_supply_values = df["total_supply"].copy()
        df = df.fillna(0)
        df["date"] = datetime.now().date()

        rows = []
        for index, row in df.iterrows():
            total_supply = total_supply_values.iloc[index]
            if total_supply == "" or total_supply is None or pd.isna(total_supply):
                total_supply = None
            else:
                total_supply = float(total_supply)

            rows.append((
                row["name"],
                row["current_price"],
                row["market_cap"],
                row["total_volume"],
                row["price_change_percentage_24h"],
                row["circulating_supply"],
                total_supply,
                row["date"],
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

    all_data = []

    for coin_name, symbol in coins.items():
        try:
            url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=5"
            response = requests.get(url)
            if response.status_code != 200:
                print(f"Error fetching {coin_name}: {response.status_code}")
                continue

            order_book = response.json()
            bids = order_book.get("bids", [])
            asks = order_book.get("asks", [])

            for i in range(min(len(bids), len(asks), 5)):
                all_data.append({
                    "coin": coin_name,
                    "level": i + 1,
                    "bid_price": float(bids[i][0]),
                    "bid_quantity": float(bids[i][1]),
                    "ask_price": float(asks[i][0]),
                    "ask_quantity": float(asks[i][1]),
                    "date": pd.Timestamp.now().date(),
                })
        except Exception as e:
            print(f"Exception for {coin_name}: {e}")
            continue

    df = pd.DataFrame(all_data, columns=[
        "coin",
        "level",
        "bid_price",
        "bid_quantity",
        "ask_price",
        "ask_quantity",
        "date",
    ])

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        rows = list(df.itertuples(index=False, name=None))
        if rows:
            cursor.executemany("""
                INSERT INTO orderbook
                (coin, level, bid_price, bid_quantity, ask_price, ask_quantity, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, rows)

        conn.commit()
        print(f"✔ orderbook inserted successfully ({len(rows)} rows)")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def run_derivatives_data():
    coins = {
        "Bitcoin": "BTCUSDT", "Ethereum": "ETHUSDT", "Binance Coin": "BNBUSDT",
        "Solana": "SOLUSDT", "XRP": "XRPUSDT", "Dogecoin": "DOGEUSDT",
        "Cardano": "ADAUSDT", "Avalanche": "AVAXUSDT", "Toncoin": "TONUSDT",
        "Polkadot": "DOTUSDT", "Polygon": "MATICUSDT", "Litecoin": "LTCUSDT",
        "Chainlink": "LINKUSDT", "TRON": "TRXUSDT", "Cosmos": "ATOMUSDT",
        "NEAR Protocol": "NEARUSDT", "Stellar": "XLMUSDT", "Filecoin": "FILUSDT",
        "Algorand": "ALGOUSDT", "ApeCoin": "APEUSDT"
    }

    data = []

    for coin_name, symbol in coins.items():
        url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                coin_data = response.json()
                data.append({
                    "coin": coin_name,
                    "mark_price": coin_data.get("markPrice"),
                    "estimated_settle_price": coin_data.get("estimatedSettlePrice"),
                    "index_price": coin_data.get("indexPrice"),
                    "funding_rate": coin_data.get("lastFundingRate"),
                    "interest_rate": coin_data.get("interestRate"),
                    "date_time": pd.Timestamp.now(),
                })
            else:
                print(f"Failed to fetch data for {coin_name} ({symbol}) - Status code {response.status_code}")
        except Exception as e:
            print(f"Exception fetching {coin_name}: {e}")

        time.sleep(0.2)

    df = pd.DataFrame(data, columns=[
        "coin",
        "mark_price",
        "estimated_settle_price",
        "index_price",
        "funding_rate",
        "interest_rate",
        "date_time",
    ])

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        rows = list(df.itertuples(index=False, name=None))
        if rows:
            cursor.executemany("""
                INSERT INTO derivatives_data
                (coin, mark_price, estimated_settle_price, index_price, funding_rate, interest_rate, date_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, rows)

        conn.commit()
        print(f"✔ derivatives_data inserted successfully ({len(rows)} rows)")
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

    all_data = []

    for coin_name, symbol in coins.items():
        url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                data["Coin"] = coin_name
                all_data.append(data)
            else:
                print(f"Error fetching {symbol}")
        except Exception as e:
            print(f"Exception fetching {coin_name}: {e}")

    df = pd.DataFrame(all_data)
    df = df.reindex(columns=[
        "Coin",
        "symbol",
        "priceChangePercent",
        "lastPrice",
        "volume",
        "openPrice",
        "highPrice",
        "lowPrice",
        "quoteVolume",
        "count",
        "bidPrice",
        "askPrice",
    ])

    df.fillna(0, inplace=True)

    price_volume_cols = [
        "priceChangePercent",
        "lastPrice",
        "volume",
        "openPrice",
        "highPrice",
        "lowPrice",
        "quoteVolume",
        "bidPrice",
        "askPrice",
    ]
    for col in price_volume_cols:
        df[col] = df[col].astype(float)

    df["count"] = df["count"].astype(float)
    df.rename(columns={"count": "trade_count"}, inplace=True)
    df["date"] = pd.Timestamp.now().date()

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        rows = [(
            row["Coin"],
            row["symbol"],
            row["priceChangePercent"],
            row["lastPrice"],
            row["volume"],
            row["openPrice"],
            row["highPrice"],
            row["lowPrice"],
            row["quoteVolume"],
            int(row["trade_count"]),
            row["bidPrice"],
            row["askPrice"],
            row["date"],
        ) for _, row in df.iterrows()]

        if rows:
            cursor.executemany("""
                INSERT INTO crypto_24market_data
                (coin, symbol, price_change_percent, last_price, volume, open_price, high_price,
                 low_price, quote_volume, trade_count, bid_price, ask_price, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, rows)

        conn.commit()
        print(f"✔ crypto_24market_data inserted successfully ({len(rows)} rows)")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def run_step(step_name, step_function):
    try:
        print(f"Running {step_name}...")
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
        run_step("crypto_data", run_crypto_data),
        run_step("orderbook", run_order_book),
        run_step("derivatives_data", run_derivatives_data),
        run_step("crypto_24market_data", run_ticker_data),
    ]

    if all(results):
        print("✔ Pipeline completed successfully.")
    else:
        print("⚠ Pipeline completed with one or more failed stages.")


if __name__ == "__main__":
    main()
