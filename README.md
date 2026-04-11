# Crypto Data Pipeline

This repository runs a daily crypto data pipeline from GitHub Actions that feeds a Power BI report.

## What it does

The pipeline:

1. Creates the required PostgreSQL tables if they do not exist.
2. Deletes rows older than 30 days.
3. Pulls crypto market data from CoinGecko.
4. Pulls order book data from Binance.
5. Pulls derivatives data from Binance Futures.
6. Pulls 24hr ticker data from Binance.

## Power BI Report

The dashboard is built in Power BI Desktop using the tables populated by this pipeline.
Link to the report:

## Local setup

1. Install Python 3.10 or newer.
2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Set the database connection string:

   - Windows PowerShell:

     ```powershell
     $env:DATABASE_URL="your-postgres-connection-string"
     ```

   - macOS/Linux:

     ```bash
     export DATABASE_URL="your-postgres-connection-string"
     ```

4. Run the pipeline:

   ```bash
   python pipeline.py
   ```

## GitHub Actions

The workflow runs automatically every day and can also be started manually.

Before the workflow can run, add a repository secret named `DATABASE_URL` with your PostgreSQL connection string.

## Files kept in the repo

- `pipeline.py` for the daily job
- `.github/workflows/crypto_pipeline.yml` for scheduling
- `requirements.txt` for dependencies

