"""Bronze: raw Debezium JSON files -> typed, append-only Parquet. One row per CDC event."""

from pyspark.sql import functions as F

from common import BRONZE_ORDERS, CHECKPOINTS, DEBEZIUM_ORDER_EVENT, RAW_ORDERS, spark_session

spark = spark_session("bronze-orders")

events = (
    spark.readStream
    .schema(DEBEZIUM_ORDER_EVENT)
    .option("maxFilesPerTrigger", 50)  # never read the whole backlog in one micro-batch
    .json(f"{RAW_ORDERS}/dt=*/hr=*/")  # the S3 sink's time-based folders
)

bronze = (
    events
    # A delete only has the old row
    .withColumn("row", F.when(F.col("op") == "d", F.col("before")).otherwise(F.col("after")))
    .select("row.*", "op", "ts_ms", F.col("source.lsn").alias("lsn"))
    .withColumn("created_at", F.to_timestamp("created_at"))  # ISO-8601 with Z, session tz is UTC
    .withColumn("order_date", F.to_date("created_at"))
)

query = (
    bronze.writeStream
    .format("parquet")
    .option("path", BRONZE_ORDERS)
    # The checkpoint remembers which raw files are done: with the sink's own log in
    # _spark_metadata, together they make bronze exactly-once
    .option("checkpointLocation", f"{CHECKPOINTS}/bronze_orders")
    .partitionBy("order_date", "channel")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)
query.awaitTermination()
