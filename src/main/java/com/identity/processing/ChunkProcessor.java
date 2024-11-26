package com.identity.processing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for processing large datasets in manageable chunks.
 */
public class ChunkProcessor {

    /**
     * Splits a large dataset into smaller chunks based on the maximum number of records per chunk.
     *
     * @param data       The input dataset.
     * @param totalCount The total number of records in the dataset.
     * @param chunkSize  The maximum number of records per chunk.
     * @param spark      SparkSession instance.
     * @return A list of dataset chunks.
     */
    public static List<Dataset<Row>> splitIntoChunks(Dataset<Row> data, long totalCount, long chunkSize, SparkSession spark) {
        List<Dataset<Row>> chunks = new ArrayList<>();

        // Calculate the number of chunks required
        long numChunks = (totalCount + chunkSize - 1) / chunkSize;

        for (int i = 0; i < numChunks; i++) {
            // Add a chunk using range filtering
            long start = i * chunkSize;
            long end = Math.min((i + 1) * chunkSize, totalCount);

            Dataset<Row> chunk = data
                    .withColumn("row_num", functions.row_number().over(
                            org.apache.spark.sql.expressions.Window.orderBy(functions.monotonically_increasing_id())))
                    .filter(functions.col("row_num").between(start + 1, end))
                    .drop("row_num");

            chunks.add(chunk);
        }

        return chunks;
    }
}
