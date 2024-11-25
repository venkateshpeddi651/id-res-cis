package com.identity.processing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Identifies the best cluster IDs by grouping records, calculating weights, and resolving conflicts.
 */
public class ClusterIdentifier {

    /**
     * Groups clusters by `Unique_ID` and calculates the best cluster ID based on index weights and Levenshtein score.
     *
     * @param data Dataset with matched records and calculated cluster IDs.
     * @return Dataset with the best cluster ID for each `Unique_ID`.
     */
    public static Dataset<Row> calculateBestClusters(Dataset<Row> data) {
        // Add index weights
        Dataset<Row> weightedData = data.withColumn(
                "weight",
                functions.expr("case when index_type = 'Name_Address_Index' then 20 " +
                               "when index_type = 'Email_Index' then 10 " +
                               "when index_type = 'Phone_Index' then 10 " +
                               "when index_type = 'MAID_Index' then 15 " +
                               "when index_type = 'DOB_Index' then 12 end")
        );

        // Group by `Unique_ID` and calculate the best cluster ID based on weights and scores
        Dataset<Row> groupedData = weightedData.groupBy("Unique_ID")
                .agg(
                        functions.first("Unique_ID").alias("client_rec_id"),
                        functions.max("weight").alias("max_weight"),
                        functions.max("levenshtein_score").alias("best_levenshtein_score"),
                        functions.first("clusterId").alias("best_cluster_id")
                );

        // Join back with original data to ensure full context
        Dataset<Row> finalData = weightedData.join(groupedData, "Unique_ID")
                .withColumn("final_cluster_id", functions.col("best_cluster_id"))
                .drop("max_weight", "best_levenshtein_score", "best_cluster_id");

        return finalData;
    }
}
