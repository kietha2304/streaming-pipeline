"""Shared paths and schemas for the lake jobs. S3A and Delta settings live in spark-defaults.conf."""

from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, DoubleType, IntegerType, LongType, StringType, StructField,
                               StructType, TimestampType)

RAW_ORDERS = "s3a://raw/topics/pg.public.orders"
BRONZE_ORDERS = "s3a://lake/bronze/orders"
SILVER_ORDERS = "s3a://lake/silver/orders"
GOLD_DAILY_SALES = "s3a://lake/gold/daily_sales_by_store"
CHECKPOINTS = "s3a://lake/_checkpoints"

# One orders row as Debezium writes it (decimal.handling.mode=double; TIMESTAMPTZ as ISO-8601 text)
ORDER_ROW = StructType([
    StructField("order_id", LongType()),
    StructField("channel", StringType()),
    StructField("store_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
    StructField("created_at", StringType()),
])

# The Debezium envelope, one JSON object per line in the raw files. The file source needs it
# up front: it doesn't infer schemas for streams.
DEBEZIUM_ORDER_EVENT = StructType([
    StructField("before", ORDER_ROW),
    StructField("after", ORDER_ROW),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
    StructField("source", StructType([StructField("lsn", LongType())])),
])

# Bronze (and silver) rows: the flattened order plus the CDC metadata
BRONZE_ORDER = StructType([
    StructField("order_id", LongType()),
    StructField("store_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
    StructField("created_at", TimestampType()),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
    StructField("lsn", LongType()),
    # partition columns, filled from the folder names
    StructField("order_date", DateType()),
    StructField("channel", StringType()),
])


def spark_session(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
