package com.identity.processing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Implements matching logic using Spark DataFrames, including Levenshtein distance
 * for both First_Name to Last_Name and Last_Name to First_Name.
 */
public class IdentityMatcher {

    public static Dataset<Row> performMatching(Dataset<Row> data) {
        // Initial blocking based on non-name fields (e.g., Email, Phone, Address)
        Dataset<Row> blockedData = data.join(data, "Email")
                .union(data.join(data, "Phone"))
                .union(data.join(data, "Address"));

        // Compute Levenshtein distance for First_Name to Last_Name
        Dataset<Row> firstToLastMatch = blockedData.withColumn(
                "levenshtein_score",
                functions.expr("1 - levenshtein(First_Name, Last_Name_prev) / greatest(length(First_Name), length(Last_Name_prev))")
        ).filter("levenshtein_score >= 0.8");

        // Compute Levenshtein distance for Last_Name to First_Name
        Dataset<Row> lastToFirstMatch = blockedData.withColumn(
                "levenshtein_score",
                functions.expr("1 - levenshtein(Last_Name, First_Name_prev) / greatest(length(Last_Name), length(First_Name_prev))")
        ).filter("levenshtein_score >= 0.8");

        // Combine the results of both matches
        Dataset<Row> combinedMatches = firstToLastMatch.union(lastToFirstMatch);

        // Add index flags (Y/N) for output schema
        Dataset<Row> finalData = combinedMatches.withColumn("Name_Address_Index_Flag", functions.lit("Y"))
                .withColumn("Email_Index_Flag", functions.when(functions.col("Email").isNotNull(), "Y").otherwise("N"))
                .withColumn("Phone_Index_Flag", functions.when(functions.col("Phone").isNotNull(), "Y").otherwise("N"))
                .withColumn("MAID_Index_Flag", functions.when(functions.col("MAID").isNotNull(), "Y").otherwise("N"))
                .withColumn("DOB_Index_Flag", functions.when(functions.col("DOB").isNotNull(), "Y").otherwise("N"));

        // Add the input schema fields along with the calculated columns
        finalData = finalData.select(
                functions.col("*") // Select all original input fields
        ).withColumn("clusterid", functions.lit(null)) // Placeholder for cluster ID assignment
         .withColumn("levenshtein_score", functions.col("levenshtein_score"))
         .withColumn("Name_Address_Index_Flag", functions.col("Name_Address_Index_Flag"))
         .withColumn("Email_Index_Flag", functions.col("Email_Index_Flag"))
         .withColumn("Phone_Index_Flag", functions.col("Phone_Index_Flag"))
         .withColumn("MAID_Index_Flag", functions.col("MAID_Index_Flag"))
         .withColumn("DOB_Index_Flag", functions.col("DOB_Index_Flag"));

        return finalData;
    }
}