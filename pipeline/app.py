import os, time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, IntegerType, LongType
import requests

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS","redpanda:9092")
PG_URL = "jdbc:postgresql://postgres:5432/appdb"
PG_USER = "app"; PG_PASS = "app"
EXTERNAL_URL = os.getenv("EXTERNAL_URL","http://external:8080/events")
CH_USER = os.getenv("CH_USER","app")
CH_PASSWORD = os.getenv("CH_PASSWORD","app")
CH_DB = os.getenv("CH_DB","analytics")

# Ensure Spark pulls Kafka & Postgres JDBC drivers
spark = (SparkSession.builder
         .appName("eventstream-fanout")
         .config("spark.jars.packages",
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                 "org.postgresql:postgresql:42.7.3")
         .getOrCreate())
spark.conf.set("spark.sql.shuffle.partitions","1")
spark.sparkContext.setLogLevel("WARN")

def get_clients():
    import redis, clickhouse_connect
    for _ in range(3):
        try:
            rds = redis.Redis(host="redis", port=6379, decode_responses=True)
            ch  = clickhouse_connect.get_client(
                    host="clickhouse", port=8123,
                    username=CH_USER, password=CH_PASSWORD, database=CH_DB)
            rds.ping(); ch.command("SELECT 1")
            return rds, ch
        except Exception:
            time.sleep(2)
    raise RuntimeError("Cannot connect to Redis/ClickHouse")

# Kafka stream (Debezium topic)
events = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", KAFKA_BROKERS)
          .option("subscribe","cdc.public.engagement_events")
          .option("startingOffsets","latest").load())

val = events.select(F.col("value").cast("string").alias("json"))
evt = val.select(
    F.get_json_object("json","$.payload.after.id").cast(LongType()).alias("event_id"),
    F.get_json_object("json","$.payload.after.content_id").cast(StringType()).alias("content_id"),
    F.get_json_object("json","$.payload.after.user_id").cast(StringType()).alias("user_id"),
    F.get_json_object("json","$.payload.after.event_type").alias("event_type"),
    F.to_timestamp(F.get_json_object("json","$.payload.after.event_ts")).alias("event_ts"),
    F.get_json_object("json","$.payload.after.duration_ms").cast(IntegerType()).alias("duration_ms"),
    F.get_json_object("json","$.payload.after.device").alias("device")
).where(F.col("event_id").isNotNull())

def process_batch(batch_df, batch_id:int):
    if batch_df.rdd.isEmpty(): 
        return

    # Fetch the content dimension via JDBC (now with explicit driver)
    content_dim = (spark.read.format("jdbc")
                   .option("url", PG_URL)
                   .option("user", PG_USER)
                   .option("password", PG_PASS)
                   .option("dbtable", "public.content")
                   .option("driver", "org.postgresql.Driver")
                   .load()
                   .select(F.col("id").cast(StringType()).alias("content_id"),
                           "content_type","length_seconds"))

    joined = (batch_df.join(F.broadcast(content_dim), "content_id","left")
              .withColumn("engagement_seconds",
                          F.when(F.col("duration_ms").isNull(), F.lit(None))
                           .otherwise((F.col("duration_ms")/1000.0).cast("double")))
              .withColumn("engagement_pct",
                          F.when(F.col("length_seconds").isNull() | F.col("duration_ms").isNull(), F.lit(None))
                           .otherwise(F.round((F.col("duration_ms")/1000.0)/F.col("length_seconds"),2))))

    rds, ch = get_clients()

    # ClickHouse sink
    import pandas as pd
    pdf = (joined.select("event_id","content_id","user_id","event_type","event_ts",
                         "duration_ms","engagement_seconds","engagement_pct",
                         "device","content_type","length_seconds").toPandas())
    if not pdf.empty:
        ch.insert_df("analytics.engagement_enriched", pdf)

    # Redis rolling 10m & leaderboard
    pipe = rds.pipeline()
    for r in joined.select("content_id").collect():
        cid = r["content_id"]
        if cid is None: continue
        key = f"eng10m:{cid}"
        pipe.incrby(key,1); pipe.expire(key,600)
    pipe.execute()
    for k in rds.scan_iter(match="eng10m:*"):
        cid = k.split(":",1)[1]
        cnt = int(rds.get(k) or 0)
        rds.zadd("top10m",{cid:cnt})

    # External webhook (idempotent)
    for r in joined.select("event_id","content_id","event_type").collect():
        idem = str(r["event_id"])
        try:
            requests.post(os.getenv("EXTERNAL_URL","http://external:8080/events"),
                          json={"event_id":r["event_id"],"content_id":r["content_id"],"event_type":r["event_type"]},
                          headers={"Idempotency-Key": idem}, timeout=1.5)
        except Exception:
            pass

query = (evt.writeStream.outputMode("append")
         .trigger(processingTime="5 seconds")
         .foreachBatch(process_batch).start())
query.awaitTermination()
