package com.identity.processing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Handles splitting large datasets into manageable chunks.
 */
public class ChunkProcessor {

    public static Dataset<Row> splitAndProcessChunks(Dataset<Row> data, long totalRecords, long maxChunkSize, SparkSession spark) {
        Dataset<Row> finalData = null;

        int numChunks = (int) Math.ceil((double) totalRecords / maxChunkSize);
        for (int i = 0; i < numChunks; i++) {
            Dataset<Row> chunk = data.limit((int) maxChunkSize).offset(i * (int) maxChunkSize);
            Dataset<Row> processedChunk = SparkApp.processChunk(chunk, spark);
            if (finalData == null) {
                finalData = processedChunk;
            } else {
                finalData = finalData.union(processedChunk);
            }
        }

        return finalData;
    }
}