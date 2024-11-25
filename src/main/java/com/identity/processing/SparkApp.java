package com.identity.processing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Main entry point for the Identity Matching Application.
 * Handles data cleansing, exploding, matching, and clustering.
 * Author: Venkatesh Peddi
 * Date: Current Date
 */
public class SparkApp {

    public static void main(String[] args) {
        // Create Spark Session
    	//TODO -> update configuration
        SparkSession spark = SparkSession.builder()
                .appName("Identity Matching Application")
                .config("spark.sql.shuffle.partitions", "200")
                .getOrCreate();

        // Validate input arguments
        if (args.length < 2) {
            System.err.println("Usage: SparkApp <input-path> <output-path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        try {
            // Load the input data
            Dataset<Row> inputData = spark.read().parquet(inputPath);

            // Check record count and split if necessary
            long recordCount = inputData.count();
            Dataset<Row> processedData;

            if (recordCount > 400_000_000) {
                // Split and process in manageable chunks
                processedData = ChunkProcessor.splitAndProcessChunks(inputData, recordCount, 400_000_000, spark);
            } else {
                // Process the entire dataset directly
                processedData = processChunk(inputData, spark);
            }

            // Save the final processed dataset
            processedData.write().mode("overwrite").parquet(outputPath);

        } catch (Exception e) {
            System.err.println("Error processing data: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }

    /**
     * Process a single chunk of data: Cleansing, exploding, matching, and clustering.
     */
    public static Dataset<Row> processChunk(Dataset<Row> chunk, SparkSession spark) {
        // Step 1: Cleanse and explode the data
        Dataset<Row> cleansedData = DataCleaner.cleanAndExplodeData(chunk);

        // Step 2: Perform identity matching
        Dataset<Row> matchedData = IdentityMatcher.performMatching(cleansedData);

        // Step 3: Identify and assign best cluster IDs
        Dataset<Row> finalData = ClusterIdentifier.calculateBestClusters(matchedData);

        return finalData;
    }
}