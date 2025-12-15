from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, sum, to_date
from pyspark.sql.types import *

#===============================================
spark = SparkSession.builder \
    .appName("EcommerceStreamingAnalytics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#===============================================
# Schema común para todos los eventos
schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("category", StringType()),
    StructField("event_type", StringType()),
    StructField("price", DoubleType()),
    StructField("timestamp", StringType()),
    StructField("store_id", StringType()),
    StructField("store_type", StringType()),
    StructField("city", StringType()),
    StructField("province", StringType()),
    StructField("location", StructType([
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType())
    ]))
])

#===============================================
def create_stream(topic_name, output_path, agg_col=None, agg_func="sum", group_cols=None):
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    json_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))
    events_df = json_df.select("data.*")
    events_df = events_df.withColumn("event_time", col("timestamp").cast("timestamp"))
    events_df = events_df.withColumn("fecha", to_date(col("event_time")))

    # Columnas para agrupar
    group_by_cols = [window(col("event_time"), "60 seconds"), col("fecha")]
    if group_cols:
        group_by_cols += [col(c) for c in group_cols]

    # Agregación
    if agg_func == "sum":
        agg_df = events_df \
            .withWatermark("event_time", "60 seconds") \
            .groupBy(*group_by_cols) \
            .sum(agg_col) \
            .withColumnRenamed(f"sum({agg_col})", f"total_{agg_col}")
    elif agg_func == "count":
        agg_df = events_df \
            .withWatermark("event_time", "60 seconds") \
            .groupBy(*group_by_cols) \
            .count()

    # Escritura en HDFS particionada por fecha
    query = agg_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", output_path + "_checkpoints") \
        .partitionBy("fecha") \
        .trigger(processingTime="60 seconds") \
        .start()

    return query

#===============================================
# 1️⃣ Orders (ventas)
orders_query = create_stream(
    topic_name="orders",
    output_path="hdfs://localhost:9000/user/luis-goncalves/ecommerce/orders",
    agg_col="price",
    agg_func="sum"
)

# 2️⃣ Cart events (agregar/quitar carrito)
cart_query = create_stream(
    topic_name="cart_events",
    output_path="hdfs://localhost:9000/user/luis-goncalves/ecommerce/cart_events",
    agg_col="user_id",  # para contar eventos
    agg_func="count",
    group_cols=["product_id", "category", "event_type"]
)

# 3️⃣ Page views (productos visitados)
page_query = create_stream(
    topic_name="page_views",
    output_path="hdfs://localhost:9000/user/luis-goncalves/ecommerce/page_views",
    agg_col="user_id",  # para contar visitas
    agg_func="count",
    group_cols=["product_id", "category"]
)

#===============================================
# Esperar a que todos los streams terminen
orders_query.awaitTermination()
cart_query.awaitTermination()
page_query.awaitTermination()
 