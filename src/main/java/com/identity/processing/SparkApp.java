package com.identity.processing;

import java.util.ArrayList;
import java.util.List;

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
            List<Dataset<Row>> processedChunks = new ArrayList<>();

            if (recordCount > 400_000_000) {
                // Process data in chunks
                List<Dataset<Row>> dataChunks = ChunkProcessor.splitIntoChunks(inputData, recordCount, 400_000_000, spark);
                for (Dataset<Row> chunk : dataChunks) {
                    processedChunks.add(processChunk(chunk, spark));
                }
            } else {
                // Process the entire dataset directly
                processedChunks.add(processChunk(inputData, spark));
            }

            // Combine all processed chunks into a single dataset
            Dataset<Row> combinedData = combineChunks(processedChunks, spark);

            // Save the final processed dataset
            combinedData.write().mode("overwrite").parquet(outputPath);


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
        Dataset<Row> cleansedData = DataHygiene.cleanAndExplodeData(chunk);
        
        // Load index tables
        Dataset<Row> emailIndex = spark.read().parquet("path_to_email_index");
        Dataset<Row> phoneIndex = spark.read().parquet("path_to_phone_index");
        Dataset<Row> maidIndex = spark.read().parquet("path_to_maid_index");
        Dataset<Row> addressIndex = spark.read().parquet("path_to_address_index");

        // Step 2: Perform identity matching
        Dataset<Row> matchedData = IdentityMatcher.performMatching(cleansedData, emailIndex, phoneIndex, maidIndex, addressIndex);

        // Step 3: Identify and assign best cluster IDs
        Dataset<Row> finalData = ClusterIdentifier.calculateBestClusters(chunk, matchedData);

        return finalData;
    }
    
    /**
     * Combines all processed chunks into a single dataset.
     *
     * @param chunks List of processed dataset chunks.
     * @param spark  SparkSession instance.
     * @return Combined dataset.
     */
    private static Dataset<Row> combineChunks(List<Dataset<Row>> chunks, SparkSession spark) {
        if (chunks.isEmpty()) {
            throw new IllegalArgumentException("No chunks to combine.");
        }

        Dataset<Row> combined = chunks.get(0); // Initialize with the first chunk
        for (int i = 1; i < chunks.size(); i++) {
            combined = combined.union(chunks.get(i));
        }

        return combined;
    }
}