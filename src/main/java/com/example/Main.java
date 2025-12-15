package com.example;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import com.example.processor.EventProcessor;
import com.example.source.KafkaSource;

public class Main {
        public static void main(String[] args) throws Exception {
                SparkSession spark = SparkSession.builder()
                                .appName("Transacciones en Tiempo Real")
                                .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint")
                                .config("spark.sql.adaptive.enabled", "true")
                                .config("spark.sql.shuffle.partitions", "10")
                                .master("local[*]")
                                .getOrCreate();

                // Reducir verbosidad de logs
                spark.sparkContext().setLogLevel("WARN");

                System.out.println("=== Aplicación de Spark Streaming iniciada ===");
                System.out.println();

                try {
                        // 1. LEER STREAM DESDE KAFKA
                        System.out.println("Paso 1: Conectando a Kafka...");
                        KafkaSource kafkaSource = new KafkaSource(spark);
                        Dataset<Row> rawStream = kafkaSource.readStream();
                        System.out.println("✓ Conexión a Kafka establecida");
                        System.out.println();

                        // 2. PROCESAR EVENTOS
                        System.out.println("Paso 2: Configurando procesamiento de eventos...");
                        EventProcessor processor = new EventProcessor(spark);

                        // Parsear JSON desde Kafka
                        Dataset<Row> parsedEvents = processor.parseEvents(rawStream);
                        System.out.println("✓ Parser de eventos configurado");

                        // Mostrar esquema de datos
                        System.out.println("\nEsquema de datos:");
                        parsedEvents.printSchema();
                        System.out.println();

                        // 3. CALCULAR MÉTRICAS
                        System.out.println("Paso 3: Configurando métricas...");
                        Dataset<Row> salesPerMinute = processor.calculateSalesPerMinute(parsedEvents);
                        Dataset<Row> topViewedProducts = processor.getTopViewedProducts(parsedEvents, 5);
                        Dataset<Row> avgTicketPerUser = processor.getAverageTicketPerUser(parsedEvents);
                        Dataset<Row> conversionRate = processor.calculateConversionRate(parsedEvents);
                        Dataset<Row> visitsByCategory = processor.getVisitsByCategory(parsedEvents);
                        Dataset<Row> demandPeaks = processor.detectDemandPeaks(parsedEvents);
                        System.out.println("✓ Métricas configuradas");
                        System.out.println();

                        // 4. AGREGACIONES PARA HDFS
                        System.out.println("Paso 4: Configurando agregaciones para almacenamiento...");
                        Dataset<Row> aggregationsByUser = processor.aggregateByUser(parsedEvents);
                        Dataset<Row> aggregationsByCategory = processor.aggregateByCategory(parsedEvents);
                        Dataset<Row> aggregationsByProduct = processor.aggregateByProduct(parsedEvents);
                        System.out.println("✓ Agregaciones configuradas");
                        System.out.println();

                        // 5. INICIAR STREAMING QUERIES
                        System.out.println("Paso 5: Iniciando queries de streaming...");
                        System.out.println("=======================================================");
                        System.out.println();

                        // Query 1: Ventas por minuto
                        System.out.println("Iniciando: Ventas por Minuto");
                        StreamingQuery salesQuery = salesPerMinute.writeStream()
                                        .outputMode("complete")
                                        .format("console")
                                        .option("truncate", false)
                                        .option("numRows", 20)
                                        .queryName("VentasPorMinuto")
                                        .trigger(Trigger.ProcessingTime("10 seconds"))
                                        .start();


                        //Almacenar en HDFS
                        Dataset<Row> salesWithTimestamp = salesPerMinute
                        .withColumn("process_date", date_format(col("minute"), "yyyy-MM-dd"))
                        .withColumn("process_hour", hour(col("minute")));

                        StreamingQuery salesHdfsQuery = salesWithTimestamp.writeStream()
                        .outputMode("append")  
                        .format("parquet")
                        .option("path", "hdfs://localhost:9000/ecommerce/analytics/sales_per_minute")
                        .option("checkpointLocation", "hdfs://localhost:9000/ecommerce/checkpoints/sales_per_minute")
                        .partitionBy("process_date") 
                        .queryName("VentasPorMinutoHDFS")
                        .trigger(Trigger.ProcessingTime("30 seconds"))
                        .start();



                        // Query 2: Top 5 productos más vistos
                        System.out.println("Iniciando: Top 5 Productos Más Vistos");
                        StreamingQuery topProductsQuery = topViewedProducts.writeStream()
                                        .outputMode("complete")
                                        .format("console")
                                        .option("truncate", false)
                                        .queryName("TopProductos")
                                        .trigger(Trigger.ProcessingTime("10 seconds"))
                                        .start();

                        //HDFS
                        StreamingQuery topProductsHdfsQuery = topViewedProducts.writeStream()
                                        .outputMode("complete")
                                        .foreachBatch((batchDF, batchId) -> {
                                                // Agregar timestamp del proceso
                                                Dataset<Row> batchWithTimestamp = batchDF
                                                                .withColumn("snapshot_timestamp", current_timestamp())
                                                                .withColumn("snapshot_date", current_date())
                                                                .withColumn("batch_id", lit(batchId));

                                                // Guardar en HDFS
                                                batchWithTimestamp.write()
                                                                .mode("append")
                                                                .partitionBy("snapshot_date")
                                                                .parquet("hdfs://localhost:9000/ecommerce/analytics/top_products");

                                                System.out.println("Batch " + batchId
                                                                + " guardado en HDFS - Top Productos");
                                        })
                                        .option("checkpointLocation",
                                                        "hdfs://localhost:9000/ecommerce/checkpoints/top_products")
                                        .queryName("TopProductosHDFS")
                                        .trigger(Trigger.ProcessingTime("30 seconds"))
                                        .start();

                        // Query 3: Ticket promedio por usuario
                        System.out.println("Iniciando: Ticket Promedio por Usuario");
                        StreamingQuery avgTicketQuery = avgTicketPerUser.writeStream()
                                        .outputMode("complete")
                                        .format("console")
                                        .option("truncate", false)
                                        .option("numRows", 10)
                                        .queryName("TicketPromedio")
                                        .trigger(Trigger.ProcessingTime("10 seconds"))
                                        .start();

                        // Query 4: Tasa de conversión
                        System.out.println("Iniciando: Tasa de Conversión");
                        StreamingQuery conversionQuery = conversionRate.writeStream()
                                        .outputMode("complete") // Keep as "complete" now that we use windows
                                        .format("console")
                                        .option("truncate", false)
                                        .queryName("TasaConversion")
                                        .trigger(Trigger.ProcessingTime("10 seconds"))
                                        .start();

                        // Query 5: Visitas por categoría
                        System.out.println("Iniciando: Visitas por Categoría");
                        StreamingQuery visitsQuery = visitsByCategory.writeStream()
                                        .outputMode("complete")
                                        .format("console")
                                        .option("truncate", false)
                                        .queryName("VisitasPorCategoria")
                                        .trigger(Trigger.ProcessingTime("10 seconds"))
                                        .start();

                        //hdfs 
                        Dataset<Row> visitsWithMetadata = visitsByCategory
                                        .withColumn("process_date", current_date())
                                        .withColumn("process_hour", hour(current_timestamp()));

                        StreamingQuery visitsHdfsQuery = visitsWithMetadata.writeStream()
                                        .outputMode("append")
                                        .format("parquet")
                                        .option("path", "hdfs://localhost:9000/ecommerce/analytics/visits_by_category")
                                        .option("checkpointLocation",
                                                        "hdfs://localhost:9000/ecommerce/checkpoints/visits_by_category")
                                        .partitionBy("process_date")
                                        .queryName("VisitasPorCategoriaHDFS")
                                        .trigger(Trigger.ProcessingTime("30 seconds"))
                                        .start();

                        // Query 6: Picos de demanda
                        System.out.println("Iniciando: Detección de Picos de DemandaIniciando: Detección de Picos de Demanda");
                        StreamingQuery peaksQuery = demandPeaks.writeStream()
                                        .outputMode("append")
                                        .format("console")
                                        .option("truncate", false)
                                        .queryName("PicosDemanda")
                                        .trigger(Trigger.ProcessingTime("10 seconds"))
                                        .start();
                        
                        //hdfs  
                        Dataset<Row> peaksWithMetadata = demandPeaks
                                        .withColumn("detection_timestamp", current_timestamp())
                                        .withColumn("detection_date", date_format(current_timestamp(), "yyyy-MM-dd"))
                                        .withColumn("detection_hour", hour(current_timestamp()));

                        StreamingQuery peaksHdfsQuery = peaksWithMetadata.writeStream()
                                        .outputMode("append")
                                        .format("parquet")
                                        .option("path", "hdfs://localhost:9000/ecommerce/analytics/demand_peaks")
                                        .option("checkpointLocation",
                                                        "hdfs://localhost:9000/ecommerce/checkpoints/demand_peaks")
                                        .partitionBy("detection_date")
                                        .queryName("PicosDemandaHDFS")
                                        .trigger(Trigger.ProcessingTime("30 seconds"))
                                        .start();

                        // Query 7: Guardar agregaciones por usuario en Parquet
                        System.out.println("Iniciando: Guardado de Agregaciones por Usuario");
                        StreamingQuery hdfsUserQuery = aggregationsByUser.writeStream()
                                        .format("parquet")
                                        .option("path", "hdfs://localhost:9000/ecommerce/users")
                                        .option("checkpointLocation", "hdfs://localhost:9000/checkpoints/users")
                                        .outputMode("append")
                                        .queryName("AggUsuarios")
                                        .trigger(Trigger.ProcessingTime("30 seconds"))
                                        .start();

                        // Query 8: Guardar agregaciones por categoría en Parquet
                        System.out.println("Iniciando: Guardado de Agregaciones por Categoría");
                        StreamingQuery hdfsCategoryQuery = aggregationsByCategory.writeStream()
                                        .format("parquet")
                                        .option("path", "hdfs://localhost:9000/ecommerce/categories")
                                        .option("checkpointLocation", "hdfs://localhost:9000/checkpoints/categories")
                                        .outputMode("append")
                                        .queryName("AggCategorias")
                                        .trigger(Trigger.ProcessingTime("30 seconds"))
                                        .start();

                        // Query 9: Guardar agregaciones por producto en Parquet
                        System.out.println("Iniciando: Guardado de Agregaciones por Producto");
                        StreamingQuery hdfsProductQuery = aggregationsByProduct.writeStream()
                                        .format("parquet")
                                        .option("path", "hdfs://localhost:9000/ecommerce/products")
                                        .option("checkpointLocation", "hdfs://localhost:9000/checkpoints/products")
                                        .outputMode("append")
                                        .queryName("AggProductos")
                                        .trigger(Trigger.ProcessingTime("30 seconds"))
                                        .start();

                        System.out.println();
                        System.out.println("=======================================================");
                        System.out.println("=== Todas las queries iniciadas correctamente ===");
                        System.out.println("=======================================================");
                        System.out.println();
                        System.out.println("Métricas activas:");
                        System.out.println("  1. Ventas por Minuto (actualización cada 10s)");
                        System.out.println("  2. Top 5 Productos Más Vistos (actualización cada 10s)");
                        System.out.println("  3. Ticket Promedio por Usuario (actualización cada 10s)");
                        System.out.println("  4. Tasa de Conversión (actualización cada 10s)");
                        System.out.println("  5. Visitas por Categoría (actualización cada 10s)");
                        System.out.println("  6. Picos de Demanda (actualización cada 10s)");
                        System.out.println();
                        System.out.println("Almacenamiento:");
                        System.out.println("  - Agregaciones por Usuario → /tmp/spark-output/users");
                        System.out.println("  - Agregaciones por Categoría → /tmp/spark-output/categories");
                        System.out.println("  - Agregaciones por Producto → /tmp/spark-output/products");
                        System.out.println();
                        System.out.println("=======================================================");
                        System.out.println("Presiona Ctrl+C para detener la aplicación");
                        System.out.println("=======================================================");
                        System.out.println("=======================================================");

                        // Esperar a que todas las queries terminen
                        spark.streams().awaitAnyTermination();

                } catch (Exception e) {
                        System.err.println("Error en la aplicación: " + e.getMessage());
                        e.printStackTrace();
                        throw e;
                } finally {
                        System.out.println("\n=== Deteniendo aplicación ===");
                        spark.stop();
                }
        }

}
