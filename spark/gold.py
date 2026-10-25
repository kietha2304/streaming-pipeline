"""Gold (option A): daily sales per channel and store, recomputed from silver in a batch loop.

Silver changes (cancellations update old rows), so a plain append stream can't feed gold. Every
few minutes this recomputes the last LOOKBACK_DAYS days and overwrites only those partitions.
"""

import os
import time
from datetime import date, timedelta

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import GOLD_DAILY_SALES, SILVER_ORDERS, spark_session

INTERVAL = int(os.getenv("GOLD_INTERVAL", "180"))  # seconds between runs
LOOKBACK_DAYS = 2

spark = spark_session("gold-daily-sales")


def refresh():
    since = (date.today() - timedelta(days=LOOKBACK_DAYS - 1)).isoformat()
    first_run = not DeltaTable.isDeltaTable(spark, GOLD_DAILY_SALES)

    silver = spark.read.format("delta").load(SILVER_ORDERS).where("status != 'cancelled'")
    if not first_run:
        silver = silver.where(F.to_date("created_at") >= since)

    daily = (
        silver
        .groupBy(F.to_date("created_at").alias("order_date"), "channel",
                 F.coalesce("store_id", F.lit(0)).alias("store_id"))  # 0 = online
        .agg(F.count("*").alias("order_count"),
             F.sum("quantity").alias("units"),
             F.round(F.sum("amount"), 2).alias("revenue"))
    )

    writer = daily.write.format("delta").mode("overwrite").partitionBy("order_date")
    if first_run:
        writer.save(GOLD_DAILY_SALES)
    else:
        # Only the recomputed days are replaced; older partitions stay as they are
        writer.option("replaceWhere", f"order_date >= '{since}'").save(GOLD_DAILY_SALES)
    print(f"[gold] refreshed {'all days' if first_run else f'days since {since}'}", flush=True)


while True:
    if DeltaTable.isDeltaTable(spark, SILVER_ORDERS):
        refresh()
    else:
        print("[gold] silver doesn't exist yet, waiting", flush=True)
    time.sleep(INTERVAL)
