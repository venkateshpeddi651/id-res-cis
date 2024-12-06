package com.identity.processing;

import com.identity.processing.constants.MatchConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Map;

/**
 * Identifies the best cluster IDs, calculates weights, handles conflicts, and merges records.
 */
public class ClusterIdentifier {

    /**
     * Resolves the best cluster ID for each record based on match weights and handles conflicts.
     *
     * @param matchedData Dataset with matched records containing `match_type`, `match_level`, and `clusterId`.
     * @return Dataset with resolved `final_cluster_id` and conflict handling.
     */
    public static Dataset<Row> calculateBestClusters(Dataset<Row> matchedData) {
        // Step 1: Add weight column based on MatchType and MatchLevel
        Dataset<Row> weightedData = matchedData.withColumn(
                "weight",
                functions.expr(buildDynamicCaseExpression("match_type", MatchConstants.MATCH_TYPE_WEIGHTS))
                        .plus(functions.expr(buildDynamicCaseExpression("match_level", MatchConstants.MATCH_LEVEL_WEIGHTS)))
        );

        // Step 2: Calculate Match Count for each record
        Dataset<Row> withMatchCount = weightedData.groupBy("Unique_ID")
                .agg(
                        functions.collect_list("match_type").alias("match_types"),
                        functions.collect_list("clusterId").alias("cluster_ids"),
                        functions.sum("weight").alias("total_weight"),
                        functions.size(functions.collect_list("match_type")).alias("Match_Count")
                );

        // Step 3: Resolve final_cluster_id and handle conflicts
        Dataset<Row> resolvedData = withMatchCount
                .withColumn(
                        "final_cluster_id",
                        functions.when(
                                functions.size(functions.col("cluster_ids")).equalTo(1),
                                functions.col("cluster_ids").getItem(0)
                        ).otherwise(null) // Blank for conflicts
                )
                .withColumn(
                        "indicator",
                        functions.when(
                                functions.size(functions.col("cluster_ids")).gt(1),
                                "I" // Set indicator to 'I' for conflicts
                        ).otherwise(null)
                );

        // Step 4: Merge records with multiple cluster IDs
        Dataset<Row> mergedData = resolvedData
                .withColumn(
                        "final_cluster_id",
                        functions.when(
                                functions.col("indicator").equalTo("I"),
                                null
                        ).otherwise(functions.col("final_cluster_id"))
                );

        return mergedData;
    }

    /**
     * Dynamically builds a CASE expression for Spark SQL based on a mapping.
     *
     * @param column Name of the column to check.
     * @param mapping Map containing values and their corresponding weights.
     * @return CASE expression as a String.
     */
    private static String buildDynamicCaseExpression(String column, Map<String, Integer> mapping) {
        StringBuilder caseExpression = new StringBuilder("CASE ");
        for (Map.Entry<String, Integer> entry : mapping.entrySet()) {
            caseExpression.append("WHEN ")
                    .append(column)
                    .append(" = '")
                    .append(entry.getKey())
                    .append("' THEN ")
                    .append(entry.getValue())
                    .append(" ");
        }
        caseExpression.append("ELSE 0 END");
        return caseExpression.toString();
    }

}
