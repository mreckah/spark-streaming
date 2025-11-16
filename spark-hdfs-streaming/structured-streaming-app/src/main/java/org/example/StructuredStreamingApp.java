package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class StructuredStreamingApp {

    public static void main(String[] args) {

        System.out.println("Starting Structured Streaming Application...");

        // 1️⃣ Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("StructuredStreamingApp")
                .master("spark://spark-master:7077")
                .getOrCreate();

        System.out.println("SparkSession created successfully");

        // 2️⃣ Define explicit schema for CSV
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, false)
                .add("value", DataTypes.DoubleType, false);

        // 3️⃣ Read CSV files as a streaming source with explicit schema
        Dataset<Row> csvStream = spark.readStream()
                .schema(schema)
                .option("header", "true")
                .option("maxFilesPerTrigger", "1")
                .csv("/data/input");

        System.out.println("CSV stream configured with schema");
        csvStream.printSchema();

        // 4️⃣ Example transformation: just select all columns
        Dataset<Row> processedStream = csvStream.select("*");

        // 5️⃣ Write stream to PostgreSQL
        try {
            StreamingQuery query = processedStream.writeStream()
                    .foreachBatch((batchDF, batchId) -> {
                        System.out.println("=================================");
                        System.out.println("Processing batch: " + batchId);
                        System.out.println("Batch row count: " + batchDF.count());
                        
                        if (!batchDF.isEmpty()) {
                            System.out.println("Data in batch:");
                            batchDF.show();
                            
                            batchDF.write()
                                    .format("jdbc")
                                    .option("url", "jdbc:postgresql://postgres:5432/mydb")
                                    .option("dbtable", "my_table")
                                    .option("user", "postgres")
                                    .option("password", "postgres")
                                    .option("driver", "org.postgresql.Driver")
                                    .mode("append")
                                    .save();
                            
                            System.out.println("✅ Batch " + batchId + " written to PostgreSQL");
                        } else {
                            System.out.println("⚠️ Batch is empty, skipping...");
                        }
                        System.out.println("=================================");
                    })
                    .option("checkpointLocation", "/opt/spark/work-dir/checkpoint")  // Changed to writable location
                    .start();

            System.out.println("🚀 Streaming query started. Waiting for data...");
            System.out.println("📂 Watching directory: /data/input");
            query.awaitTermination();

        } catch (StreamingQueryException | TimeoutException e) {
            System.err.println("❌ Error in streaming query:");
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}