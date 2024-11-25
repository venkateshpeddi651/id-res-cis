package com.identity.processing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Calculates the best cluster IDs for each record.
 */
public class ClusterIdentifier {

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

        // Resolve conflicts using Levenshtein score and weight
        Dataset<Row> resolvedClusters = weightedData.orderBy(
                functions.desc("weight"),
                functions.desc("levenshtein_score"),
                functions.asc("clusterId")
        );

        // Include clusterId in the output schema
        return resolvedClusters.withColumn("clusterid", functions.col("clusterId"));
    }
}