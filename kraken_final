# Kraken Data Pipeline

This project implements a data pipeline for cryptocurrency market data using Apache Airflow, Kafka, and SQLite.

## Architecture Overview

The system consists of three Airflow DAGs:

### DAG 1 – Continuous Ingestion
- Fetches market data from the Kraken API every minute
- Sends raw JSON messages to Kafka topic `raw_events`

Flow:
API → DAG 1 → Kafka (raw_events)

### DAG 2 – Hourly Cleaning & Storage
- Runs hourly
- Reads new messages from Kafka
- Cleans and normalizes data
- Stores cleaned data in SQLite table `events`

Flow:
Kafka → DAG 2 → SQLite (events)

### DAG 3 – Daily Analytics
- Runs daily
- Reads cleaned data from SQLite
- Computes aggregated analytics
- Stores results in SQLite table `daily_summary`

Flow:
SQLite (events) → DAG 3 → SQLite (daily_summary)

## Technologies
- Python
- Apache Airflow
- Apache Kafka
- SQLite
- Docker & Docker Compose

## How to Run
```bash
docker compose up -d --build
