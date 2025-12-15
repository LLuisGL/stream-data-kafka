package com.example.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KafkaSource {

    private final SparkSession spark;
    private final String bootstrapServers;
    private final String topic;

    public KafkaSource(SparkSession spark) {
        this(spark, "localhost:9092", "stream_in");
    }

    public KafkaSource(SparkSession spark, String bootstrapServers, String topic) {
        this.spark = spark;
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
    }

    public Dataset<Row> readStream() {
        System.out.println("=== Conectando a Kafka ===");
        System.out.println("Bootstrap servers: " + bootstrapServers);
        System.out.println("Topic: " + topic);
        
        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topic)
                .option("startingOffsets", "latest")  // Lee solo mensajes nuevos
                .option("failOnDataLoss", "false")    // No fallar si se pierde data
                .option("maxOffsetsPerTrigger", "1000") // Límite de mensajes por batch
                .load();
    }

    public Dataset<Row> readStreamMultipleTopics(String[] topics) {
        String topicsStr = String.join(",", topics);
        System.out.println("=== Conectando a Kafka (múltiples topics) ===");
        System.out.println("Bootstrap servers: " + bootstrapServers);
        System.out.println("Topics: " + topicsStr);
        
        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topicsStr)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load();
    }

    public Dataset<Row> readStreamFromBeginning() {
        System.out.println("=== Conectando a Kafka desde el inicio ===");
        System.out.println("Bootstrap servers: " + bootstrapServers);
        System.out.println("Topic: " + topic);
        
        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")  // Lee desde el principio
                .option("failOnDataLoss", "false")
                .load();
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

}
