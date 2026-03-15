import os
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
}

COINGECKO_URL = os.getenv("COINGECKO_API_URL")

COINS = ["bitcoin", "ethereum", "solana", "cardano", "polkadot"]


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def create_tables():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS coins (
            id VARCHAR(50) PRIMARY KEY,
            symbol VARCHAR(20),
            name VARCHAR(100)
        );

        CREATE TABLE IF NOT EXISTS prices (
            id SERIAL PRIMARY KEY,
            coin_id VARCHAR(50) REFERENCES coins(id),
            price_usd NUMERIC(20, 8),
            market_cap NUMERIC(30, 2),
            volume_24h NUMERIC(30, 2),
            price_change_24h NUMERIC(10, 4),
            fetched_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Tables ready.")


def fetch_market_data():
    url = f"{COINGECKO_URL}/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": ",".join(COINS),
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": False,
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def transform(data):
    rows = []
    for coin in data:
        rows.append({
            "id": coin["id"],
            "symbol": coin["symbol"],
            "name": coin["name"],
            "price_usd": coin["current_price"],
            "market_cap": coin["market_cap"],
            "volume_24h": coin["total_volume"],
            "price_change_24h": coin["price_change_percentage_24h"],
        })
    return pd.DataFrame(rows)


def load(df):
    conn = get_connection()
    cur = conn.cursor()

    # Upsert coins
    coin_data = df[["id", "symbol", "name"]].values.tolist()
    execute_values(cur, """
        INSERT INTO coins (id, symbol, name)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            symbol = EXCLUDED.symbol,
            name = EXCLUDED.name
    """, coin_data)

    # Insert prices
    price_data = df[["id", "price_usd", "market_cap", "volume_24h", "price_change_24h"]].values.tolist()
    execute_values(cur, """
        INSERT INTO prices (coin_id, price_usd, market_cap, volume_24h, price_change_24h)
        VALUES %s
    """, price_data)

    conn.commit()
    cur.close()
    conn.close()
    print(f"[{datetime.now()}] Data loaded successfully.")


def run():
    print(f"[{datetime.now()}] Starting ingestion...")
    create_tables()
    data = fetch_market_data()
    df = transform(data)
    load(df)


if __name__ == "__main__":
    run()