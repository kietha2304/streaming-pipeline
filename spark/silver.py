"""Silver: one row per order, its latest version, as a Delta table. CDC changes merged by hand.

Reads bronze rather than the raw files: the JSON is already parsed and typed once, and bronze is
the replayable history, so silver can be rebuilt without touching raw.
"""

import time

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import BRONZE_ORDER, BRONZE_ORDERS, CHECKPOINTS, SILVER_ORDERS, spark_session

spark = spark_session("silver-orders")


def upsert_orders(batch_df, batch_id):
    # 1. Within the batch, keep only the latest event per order (an insert and its cancel can
    #    land in the same micro-batch)
    latest = Window.partitionBy("order_id").orderBy(F.col("lsn").desc(), F.col("ts_ms").desc())
    changes = (
        batch_df
        .withColumn("rn", F.row_number().over(latest))
        .where("rn = 1")
        .drop("rn", "order_date")
    )

    # First batch: create the table from the orders that still exist
    if not DeltaTable.isDeltaTable(spark, SILVER_ORDERS):
        changes.where("op != 'd'").write.format("delta").save(SILVER_ORDERS)
        return

    # 2. Merge. `s.lsn > t.lsn` makes a replayed old event (at-least-once) a no-op, instead of
    #    overwriting a newer version with an older one.
    (
        DeltaTable.forPath(spark, SILVER_ORDERS).alias("t")
        .merge(changes.alias("s"), "t.order_id = s.order_id")
        .whenMatchedDelete(condition="s.op = 'd' AND s.lsn > t.lsn")
        .whenMatchedUpdateAll(condition="s.op != 'd' AND s.lsn > t.lsn")
        .whenNotMatchedInsertAll(condition="s.op != 'd'")
        .execute()
    )


def bronze_exists():
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(f"{BRONZE_ORDERS}/_spark_metadata")
    return path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).exists(path)


# A streaming read of a folder that doesn't exist yet fails at startup
while not bronze_exists():
    print("[silver] bronze doesn't exist yet, waiting", flush=True)
    time.sleep(10)

query = (
    spark.readStream
    .schema(BRONZE_ORDER)
    .option("maxFilesPerTrigger", 100)
    .parquet(BRONZE_ORDERS)
    .writeStream
    .foreachBatch(upsert_orders)
    .option("checkpointLocation", f"{CHECKPOINTS}/silver_orders")
    .trigger(processingTime="1 minute")
    .start()
)
query.awaitTermination()
