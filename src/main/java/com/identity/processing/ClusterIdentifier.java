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

        // Step 2: Identify Unique_ID groups with only "IP_ONLY" match_type
        Dataset<Row> ipOnlyGroups = weightedData.groupBy("Unique_ID")
                .agg(
                        functions.collect_set("match_type").alias("unique_match_types")
                )
                .withColumn(
                        "is_ip_only",
                        functions.when(functions.array_contains(functions.col("unique_match_types"), "IP_ONLY")
                                .and(functions.size(functions.col("unique_match_types")).equalTo(1)), true)
                                .otherwise(false)
                )
                .select("Unique_ID", "is_ip_only");

        // Step 3: Filter out IP_ONLY groups and handle them separately
        Dataset<Row> nonIpOnlyData = weightedData.join(ipOnlyGroups, "Unique_ID")
                .filter(functions.col("is_ip_only").equalTo(false))
                .drop("is_ip_only");

        Dataset<Row> ipOnlyData = weightedData.join(ipOnlyGroups, "Unique_ID")
                .filter(functions.col("is_ip_only").equalTo(true))
                .drop("is_ip_only");

        // Step 4: Calculate Match Count and resolve final_cluster_id for non-IP_ONLY data
        Dataset<Row> groupedData = nonIpOnlyData.groupBy("Unique_ID")
                .agg(
                        functions.collect_list("match_type").alias("match_types"),
                        functions.collect_list("clusterId").alias("cluster_ids"),
                        functions.sum("weight").alias("total_weight"),
                        functions.size(functions.collect_list("match_type")).alias("Match_Count")
                );

        Dataset<Row> resolvedData = groupedData
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

        // Step 5: Merge records with multiple cluster IDs
        Dataset<Row> mergedData = resolvedData
                .withColumn(
                        "final_cluster_id",
                        functions.when(
                                functions.col("indicator").equalTo("I"),
                                null
                        ).otherwise(functions.col("final_cluster_id"))
                );

        // Step 6: Combine non-IP_ONLY and IP_ONLY data for final output
        Dataset<Row> finalIpOnlyData = ipOnlyData.withColumn("indicator", functions.lit(null));
        Dataset<Row> finalOutput = mergedData.unionByName(finalIpOnlyData);

        return finalOutput;
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
