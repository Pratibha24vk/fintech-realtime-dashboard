"""
Simulated Stock Trade Event Producer
Generates realistic trade events and publishes them to a Kafka topic.
Usage: python producer.py --tickers AAPL,MSFT,GOOGL --rate 100
"""

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "raw-trades"

# Simulated base prices
BASE_PRICES = {
    "AAPL": 182.50, "MSFT": 415.20, "GOOGL": 175.80,
    "AMZN": 185.60, "TSLA": 175.30, "NVDA": 875.40,
    "META": 510.20, "JPM": 200.10, "BAC": 38.90, "GS": 450.00,
}


def generate_trade_event(ticker: str, last_price: float) -> dict:
    """Simulate a realistic trade tick with price walk."""
    price_change_pct = random.gauss(0, 0.0005)          # ~0.05% std per tick
    new_price = round(last_price * (1 + price_change_pct), 4)
    volume = random.randint(100, 5000)

    return {
        "event_id":   str(uuid.uuid4()),
        "ticker":     ticker,
        "price":      new_price,
        "volume":     volume,
        "side":       random.choice(["BUY", "SELL"]),
        "exchange":   random.choice(["NYSE", "NASDAQ", "CBOE"]),
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed for {msg.key()}: {err}")


def run_producer(tickers: list[str], rate: int) -> None:
    """Produce trade events at the given rate (events/sec)."""
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    prices = {t: BASE_PRICES.get(t, 100.0) for t in tickers}
    interval = 1.0 / rate

    logger.info(f"Publishing to topic '{TOPIC}' at {rate} events/sec")
    logger.info(f"Tickers: {tickers}")

    try:
        while True:
            ticker = random.choice(tickers)
            event = generate_trade_event(ticker, prices[ticker])
            prices[ticker] = event["price"]          # walk the price

            producer.produce(
                topic=TOPIC,
                key=ticker,
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.poll(0)
            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        producer.flush()
        logger.info("Producer flushed and closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulated trade event producer")
    parser.add_argument("--tickers", default="AAPL,MSFT,GOOGL,AMZN,TSLA",
                        help="Comma-separated list of ticker symbols")
    parser.add_argument("--rate", type=int, default=100,
                        help="Events per second")
    args = parser.parse_args()

    run_producer(args.tickers.split(","), args.rate)
