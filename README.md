# 📈 Fintech Real-Time Trading Dashboard

A real-time streaming pipeline that ingests simulated stock trade events, processes them with Apache Spark Structured Streaming, and serves live financial metrics to a Power BI / Tableau dashboard.

---

## 🏗️ Architecture

```
[Simulated Trade Producer]
         │  (Kafka topic: raw-trades)
         ▼
[Apache Kafka — Broker]
         │
         ▼
[Spark Structured Streaming]
    ├── Windowed aggregations (VWAP, volume, volatility)
    ├── Watermarking for late data
    └── Output → Delta Lake / Snowflake
         │
         ▼
[Serving Layer — Delta / Snowflake]
         │
         ▼
[Power BI / Tableau — Live Dashboard]
```

---

## 🧰 Tech Stack

| Layer | Tool |
|---|---|
| Message Broker | Apache Kafka (Confluent) |
| Stream Processing | Apache Spark 3.5 Structured Streaming |
| Storage | Delta Lake (S3) + Snowflake |
| Orchestration | Airflow (batch reconciliation) |
| Visualization | Power BI / Tableau |
| Containerization | Docker + Docker Compose |
| Language | Python (PySpark) |

---

## 📁 Project Structure

```
fintech-realtime-dashboard/
├── kafka/
│   ├── producer.py               # Simulated trade event producer
│   └── topic_setup.py            # Kafka topic creation script
├── spark/
│   ├── streaming_job.py          # Main Spark Structured Streaming job
│   └── schemas.py                # Pydantic / StructType schemas
├── dashboard/
│   ├── metrics_query.sql         # Key metric SQL queries for BI tools
│   └── README.md                 # Dashboard setup instructions
├── docker/
│   ├── docker-compose.yml        # Kafka + Spark + Zookeeper setup
│   └── Dockerfile.spark
├── .github/
│   └── workflows/
│       └── spark_ci.yml
├── requirements.txt
└── README.md
```

---

## 📊 Metrics Produced (Real-Time)

| Metric | Description | Window |
|---|---|---|
| VWAP | Volume-Weighted Average Price per ticker | 1-min, 5-min |
| Trade Volume | Number of shares traded | 1-min rolling |
| Price Volatility | Std deviation of trade price | 5-min |
| Bid-Ask Spread | Simulated spread per ticker | Tick-level |
| Top Movers | Tickers with largest % price change | 5-min |
| Alert Flags | Price spike > 3σ from rolling mean | Real-time |

---

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.10+
- Snowflake or Delta Lake (S3) account

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/fintech-realtime-dashboard.git
cd fintech-realtime-dashboard
```

### 2. Start Kafka + Spark
```bash
docker-compose up -d
```

### 3. Create Kafka topic
```bash
python kafka/topic_setup.py
```

### 4. Start the trade producer
```bash
python kafka/producer.py --tickers AAPL,MSFT,GOOGL,AMZN,TSLA --rate 100
```

### 5. Submit the Spark streaming job
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
io.delta:delta-core_2.12:2.4.0 \
  spark/streaming_job.py
```

### 6. Connect your BI tool
Point Power BI or Tableau to the Snowflake `ANALYTICS.TRADE_METRICS` table with DirectQuery / Live Connection enabled.

---

## ⚡ Spark Job Highlights
- **Watermarking** — 10-second watermark handles late-arriving events gracefully
- **Windowed aggregations** — Tumbling 1-min and sliding 5-min windows
- **Exactly-once semantics** — Checkpoint location ensures no duplicate processing
- **Schema enforcement** — All incoming Kafka messages validated against a strict StructType

---

## 🔔 Alerting
Price spike alerts (> 3σ from 5-min rolling mean) are written to a separate Kafka topic `trade-alerts` and can be routed to Slack / PagerDuty via a consumer.

---

## 📚 Resources
- [Spark Structured Streaming Docs](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Confluent Kafka Python Client](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Delta Lake](https://delta.io/)
