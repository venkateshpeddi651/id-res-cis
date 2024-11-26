package com.identity.processing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Identifies the best cluster IDs and ensures 1:1 mapping for input records.
 */
public class ClusterIdentifier {

    /**
     * Groups clusters by `Unique_ID`, resolves the best cluster ID, and joins back with original data.
     *
     * @param originalData Original client dataset (before transformations).
     * @param matchedData  Dataset with matched records and calculated cluster IDs.
     * @return Dataset with the best cluster ID joined back with the original data.
     */
    public static Dataset<Row> calculateBestClusters(Dataset<Row> originalData, Dataset<Row> matchedData) {
        // Add index weights
        Dataset<Row> weightedData = matchedData.withColumn(
                "weight",
                functions.expr("case when index_type = 'Name_Address_Index' then 20 " +
                               "when index_type = 'Email_Index' then 10 " +
                               "when index_type = 'Phone_Index' then 10 " +
                               "when index_type = 'MAID_Index' then 15 " +
                               "when index_type = 'DOB_Index' then 12 end")
        );

        // Resolve to 1 record per Unique_ID by selecting the best match
        Dataset<Row> deduplicatedData = weightedData.groupBy("Unique_ID")
                .agg(
                        functions.first("Unique_ID").alias("client_rec_id"), // Alias for clarity
                        functions.first("Generated_Record_ID").alias("Generated_Record_ID"),
                        functions.max("weight").alias("max_weight"),
                        functions.first("clusterId").alias("best_cluster_id"),
                        functions.max("levenshtein_score").alias("best_levenshtein_score")
                );

        // Join back with original data to ensure full context
        Dataset<Row> finalOutput = originalData.join(deduplicatedData, "Unique_ID")
                .withColumn("final_cluster_id", functions.col("best_cluster_id"));

        return finalOutput;
    }
}
