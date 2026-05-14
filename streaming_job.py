"""
Spark Structured Streaming — Trade Metrics Job
Reads from Kafka topic 'raw-trades', computes windowed financial metrics,
and writes results to Delta Lake and/or Snowflake.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, TimestampType
)

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC     = "raw-trades"
CHECKPOINT_PATH = "s3://your-bucket/checkpoints/trade-metrics"
DELTA_OUTPUT    = "s3://your-bucket/delta/trade-metrics"


# ── Schema ─────────────────────────────────────────────────────────────────

TRADE_SCHEMA = StructType([
    StructField("event_id",  StringType(),    False),
    StructField("ticker",    StringType(),    False),
    StructField("price",     DoubleType(),    False),
    StructField("volume",    LongType(),      False),
    StructField("side",      StringType(),    True),
    StructField("exchange",  StringType(),    True),
    StructField("timestamp", TimestampType(), False),
])


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FinTech-Trade-Metrics")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.col("key").cast("string").alias("ticker_key"),
            F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("data"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select("data.*")
        .withColumn("event_time", F.col("timestamp").cast(TimestampType()))
        .withWatermark("event_time", "10 seconds")
    )


def compute_vwap(trades_df):
    """Volume-Weighted Average Price — 1-minute tumbling window."""
    return (
        trades_df
        .groupBy(
            F.window("event_time", "1 minute").alias("window"),
            F.col("ticker"),
        )
        .agg(
            (F.sum(F.col("price") * F.col("volume")) / F.sum("volume"))
                .alias("vwap"),
            F.sum("volume").alias("total_volume"),
            F.count("event_id").alias("trade_count"),
            F.min("price").alias("low"),
            F.max("price").alias("high"),
            F.last("price").alias("close"),
            F.first("price").alias("open"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "ticker", "vwap", "total_volume", "trade_count",
            "low", "high", "open", "close",
        )
    )


def compute_volatility(trades_df):
    """Price volatility (std dev) — 5-minute sliding window."""
    return (
        trades_df
        .groupBy(
            F.window("event_time", "5 minutes", "1 minute").alias("window"),
            F.col("ticker"),
        )
        .agg(
            F.stddev("price").alias("price_volatility"),
            F.avg("price").alias("avg_price"),
            F.count("event_id").alias("trade_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "ticker", "price_volatility", "avg_price", "trade_count",
        )
    )


def write_to_delta(df, output_path: str, checkpoint_suffix: str, mode: str = "append"):
    return (
        df.writeStream
        .format("delta")
        .outputMode(mode)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{checkpoint_suffix}")
        .option("path", f"{output_path}/{checkpoint_suffix}")
        .trigger(processingTime="10 seconds")
        .start()
    )


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    trades = read_kafka_stream(spark)

    vwap_df       = compute_vwap(trades)
    volatility_df = compute_volatility(trades)

    q1 = write_to_delta(vwap_df,       DELTA_OUTPUT, "vwap",       mode="append")
    q2 = write_to_delta(volatility_df, DELTA_OUTPUT, "volatility", mode="append")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
