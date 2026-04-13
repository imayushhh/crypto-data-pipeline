# Crypto Data Pipeline

This repository runs a daily crypto ingestion pipeline from GitHub Actions and loads the results into PostgreSQL for a Power BI report.

## What It Does

The current `pipeline.py` performs four stages:

1. Creates the required PostgreSQL tables if they do not already exist.
2. Deletes data older than 30 days from every table.
3. Pulls spot market data from CoinGecko into `crypto_data`.
4. Pulls order book data from Binance spot hosts into `orderbook`.
5. Pulls derivatives data from OKX into `derivatives_data`.
6. Pulls 24-hour ticker data from Binance spot hosts into `crypto_24market_data`.

Each stage is isolated so one failed source does not stop the rest of the pipeline.

## Data Sources

- `crypto_data`: CoinGecko markets API.
- `orderbook`: Binance spot depth endpoint, with host fallback.
- `derivatives_data`: OKX public market APIs.
- `crypto_24market_data`: Binance spot 24hr ticker endpoint, with host fallback.

## Power BI Report





## GitHub Actions

The workflow runs automatically every day at 08:00 UTC and can also be started manually.

Before the workflow can run, add a repository secret named `DATABASE_URL` with your PostgreSQL connection string.

## Repository Contents

- `pipeline.py` for the daily job.
- `.github/workflows/crypto_pipeline.yml` for scheduling.
- `requirements.txt` for Python dependencies.

## Notes

- No Airflow scheduler or Docker setup is required anymore.
- The pipeline uses public APIs only; no API keys are needed for the current stages.
- If Binance spot is unavailable from the runner, the pipeline falls back to alternate hosts for the order book and ticker stages.

