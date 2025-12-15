package com.example.processor;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.window;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class EventProcessor {
    private SparkSession spark;

    public EventProcessor(SparkSession spark) {
        this.spark = spark;
    }

    public Dataset<Row> parseEvents(Dataset<Row> rawStream) {
        Dataset<Row> stringStream = rawStream.selectExpr("CAST(value AS STRING) as json");

        StructType schema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[] {
                DataTypes.createStructField("user_id", DataTypes.IntegerType, true),
                DataTypes.createStructField("product_id", DataTypes.IntegerType, true),
                DataTypes.createStructField("category", DataTypes.StringType, true),
                DataTypes.createStructField("event_type", DataTypes.StringType, true),
                DataTypes.createStructField("price", DataTypes.DoubleType, true),
                DataTypes.createStructField("timestamp", DataTypes.TimestampType, true),
                DataTypes.createStructField("store_id", DataTypes.StringType, true),
                DataTypes.createStructField("store_type", DataTypes.StringType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("province", DataTypes.StringType, true),
                DataTypes.createStructField("location",
                        DataTypes.createStructType(new org.apache.spark.sql.types.StructField[] {
                                DataTypes.createStructField("lat", DataTypes.DoubleType, true),
                                DataTypes.createStructField("lon", DataTypes.DoubleType, true)
                        }), true)
        });

        return stringStream
                .select(from_json(col("json"), schema).as("data"))
                .select("data.*")
                .withColumn("event_time", to_timestamp(col("timestamp")))
                .withWatermark("event_time", "2 minutes"); 
    }    
    /**
     * 1. Ventas totales por minuto
     */
    public Dataset<Row> calculateSalesPerMinute(Dataset<Row> events) {
        return events
                .filter(col("event_type").equalTo("purchase"))
                .groupBy(window(col("event_time"), "1 minute"))
                .agg(
                        sum("price").as("total_sales"),
                        count("*").as("total_transactions"),
                        avg("price").as("avg_transaction")
                )
                .select(
                        col("window.start").as("minute"),
                        col("total_sales"),
                        col("total_transactions"),
                        round(col("avg_transaction"), 2).as("avg_transaction")
                )
                .orderBy(col("minute").desc());
    }

    /**
     * 2. Top N productos más vistos
     */
    public Dataset<Row> getTopViewedProducts(Dataset<Row> events, int topN) {
        return events
                .filter(col("event_type").equalTo("view"))
                .groupBy("product_id", "category")
                .agg(count("*").as("view_count"))
                .orderBy(col("view_count").desc())
                .limit(topN);
    }

    /**
     * 3. Promedio de ticket de compra por usuario
     */
    public Dataset<Row> getAverageTicketPerUser(Dataset<Row> events) {
        return events
                .filter(col("event_type").equalTo("purchase"))
                .groupBy("user_id")
                .agg(
                        avg("price").as("avg_ticket"),
                        sum("price").as("total_spent"),
                        count("*").as("purchase_count")
                )
                .select(
                        col("user_id"),
                        round(col("avg_ticket"), 2).as("avg_ticket"),
                        round(col("total_spent"), 2).as("total_spent"),
                        col("purchase_count")
                )
                .orderBy(col("total_spent").desc());
    }

    /**
     * 4. Conversión entre vista -> carrito -> compra
     */
    public Dataset<Row> calculateConversionRate(Dataset<Row> events) {
        // Use windowed aggregation to respect watermark
        return events
                .groupBy(window(col("event_time"), "1 minute"))
                .agg(
                        sum(when(col("event_type").equalTo("view"), 1).otherwise(0)).as("views"),
                        sum(when(col("event_type").equalTo("add_to_cart"), 1).otherwise(0)).as("carts"),
                        sum(when(col("event_type").equalTo("purchase"), 1).otherwise(0)).as("purchases")
                )
                .select(
                        col("window.start").as("minute"),
                        col("views"),
                        col("carts"),
                        col("purchases"),
                        round(
                                when(col("views").gt(0), 
                                    col("carts").multiply(100.0).divide(col("views"))
                                ).otherwise(0.0), 
                                2
                        ).as("view_to_cart_rate"),
                        round(
                                when(col("carts").gt(0), 
                                    col("purchases").multiply(100.0).divide(col("carts"))
                                ).otherwise(0.0), 
                                2
                        ).as("cart_to_purchase_rate"),
                        round(
                                when(col("views").gt(0), 
                                    col("purchases").multiply(100.0).divide(col("views"))
                                ).otherwise(0.0), 
                                2
                        ).as("overall_conversion")
                )
                .orderBy(col("minute").desc());
    }

    /**
     * 5. Conteo de visitas por categoría
     */
    public Dataset<Row> getVisitsByCategory(Dataset<Row> events) {
        return events
                .filter(col("event_type").isin("view", "add_to_cart", "purchase"))
                .groupBy(window(col("timestamp"), "1 minute").alias("win"), col("category"))
                .agg(
                        sum(when(col("event_type").equalTo("view"), 1).otherwise(0)).alias("views"),
                        sum(when(col("event_type").equalTo("add_to_cart"), 1).otherwise(0)).alias("add_to_cart"),
                        sum(when(col("event_type").equalTo("purchase"), 1).otherwise(0)).alias("purchases")
                )
                .select(
                        col("win.start").alias("window_start"),
                        col("win.end").alias("window_end"),
                        col("category"),
                        col("views"),
                        col("add_to_cart"),
                        col("purchases")
                );
    }

    /**
     * 6. Detección de picos de demanda (más de 10 eventos en 1 minuto)
     */
    public Dataset<Row> detectDemandPeaks(Dataset<Row> events) {
        return events
                .groupBy(
                        window(col("event_time"), "1 minute"),
                        col("category"),
                        col("city")
                )
                .agg(count("*").as("event_count"))
                .filter(col("event_count").gt(10))
                .select(
                        col("window.start").as("peak_time"),
                        col("category"),
                        col("city"),
                        col("event_count")
                );
    }

    /**
     * 7. Agregaciones por usuario (para HDFS)
     */
    public Dataset<Row> aggregateByUser(Dataset<Row> events) {
        return events
                .groupBy(
                        window(col("event_time"), "5 minutes"),
                        col("user_id")
                )
                .agg(
                        count(when(col("event_type").equalTo("view"), 1)).as("total_views"),
                        count(when(col("event_type").equalTo("add_to_cart"), 1)).as("total_carts"),
                        count(when(col("event_type").equalTo("purchase"), 1)).as("total_purchases"),
                        sum(when(col("event_type").equalTo("purchase"), col("price")).otherwise(0)).as("total_spent"),
                        avg(when(col("event_type").equalTo("purchase"), col("price"))).as("avg_ticket")
                )
                .select(
                        col("window.start").as("window_start"),
                        col("window.end").as("window_end"),
                        col("user_id"),
                        col("total_views"),
                        col("total_carts"),
                        col("total_purchases"),
                        round(col("total_spent"), 2).as("total_spent"),
                        round(col("avg_ticket"), 2).as("avg_ticket")
                );
    }

    /**
     * 8. Agregaciones por categoría (para HDFS)
     */
    public Dataset<Row> aggregateByCategory(Dataset<Row> events) {
        return events
                .groupBy(
                        window(col("event_time"), "5 minutes"),
                        col("category")
                )
                .agg(
                        count(when(col("event_type").equalTo("view"), 1)).as("total_views"),
                        count(when(col("event_type").equalTo("add_to_cart"), 1)).as("total_carts"),
                        count(when(col("event_type").equalTo("purchase"), 1)).as("total_purchases"),
                        sum(when(col("event_type").equalTo("purchase"), col("price")).otherwise(0)).as("total_revenue"),
                        avg(when(col("event_type").equalTo("purchase"), col("price"))).as("avg_price")
                )
                .select(
                        col("window.start").as("window_start"),
                        col("window.end").as("window_end"),
                        col("category"),
                        col("total_views"),
                        col("total_carts"),
                        col("total_purchases"),
                        round(col("total_revenue"), 2).as("total_revenue"),
                        round(col("avg_price"), 2).as("avg_price")
                );
    }

    /**
     * 9. Agregaciones por producto (para HDFS)
     */
    public Dataset<Row> aggregateByProduct(Dataset<Row> events) {
        return events
                .groupBy(
                        window(col("event_time"), "5 minutes"),
                        col("product_id"),
                        col("category")
                )
                .agg(
                        count(when(col("event_type").equalTo("view"), 1)).as("total_views"),
                        count(when(col("event_type").equalTo("add_to_cart"), 1)).as("total_carts"),
                        count(when(col("event_type").equalTo("purchase"), 1)).as("total_purchases"),
                        sum(when(col("event_type").equalTo("purchase"), col("price")).otherwise(0)).as("total_revenue"),
                        avg(when(col("event_type").equalTo("purchase"), col("price"))).as("avg_price"),
                        max(col("price")).as("max_price"),
                        min(col("price")).as("min_price")
                )
                .select(
                        col("window.start").as("window_start"),
                        col("window.end").as("window_end"),
                        col("product_id"),
                        col("category"),
                        col("total_views"),
                        col("total_carts"),
                        col("total_purchases"),
                        round(col("total_revenue"), 2).as("total_revenue"),
                        round(col("avg_price"), 2).as("avg_price"),
                        round(col("max_price"), 2).as("max_price"),
                        round(col("min_price"), 2).as("min_price")
                );
    }

}
