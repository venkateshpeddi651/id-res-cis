package com.identity.processing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Implements identity matching logic using Spark DataFrames.
 * Handles joins based on index definitions and filters using Levenshtein distances for approximate name matching.
 */
public class IdentityMatcher {

    public static Dataset<Row> performMatching(Dataset<Row> clientData,
                                               Dataset<Row> emailIndex,
                                               Dataset<Row> phoneIndex,
                                               Dataset<Row> maidIndex,
                                               Dataset<Row> addressIndex) {
        // Step 1: Perform initial joins on non-name columns
        Dataset<Row> reducedData = performInitialJoins(clientData, emailIndex, phoneIndex, maidIndex, addressIndex);

        // Step 2: Filter candidates by name matching with Levenshtein distance
        Dataset<Row> nameMatchedData = filterByNameLevenshtein(reducedData);

        return nameMatchedData;
    }

    private static Dataset<Row> performInitialJoins(Dataset<Row> clientData,
                                                    Dataset<Row> emailIndex,
                                                    Dataset<Row> phoneIndex,
                                                    Dataset<Row> maidIndex,
                                                    Dataset<Row> addressIndex) {
        // Join on Email
        Dataset<Row> emailJoined = clientData.join(emailIndex,
                clientData.col("Email_Address_One").equalTo(emailIndex.col("TU_email"))
                        .or(clientData.col("Email_Address_Two").equalTo(emailIndex.col("TU_email")))
                        .or(clientData.col("Email_Address_Three").equalTo(emailIndex.col("TU_email"))),
                "left_outer");

        // Join on Phone
        Dataset<Row> phoneJoined = emailJoined.join(phoneIndex,
                emailJoined.col("Phone_Number_One").equalTo(phoneIndex.col("TU_phone_nbr"))
                        .or(emailJoined.col("Phone_Number_Two").equalTo(phoneIndex.col("TU_phone_nbr")))
                        .or(emailJoined.col("Phone_Number_Three").equalTo(phoneIndex.col("TU_phone_nbr"))),
                "left_outer");

        // Join on MAID
        Dataset<Row> maidJoined = phoneJoined.join(maidIndex,
                phoneJoined.col("MAID_One").equalTo(maidIndex.col("TU_maid"))
                        .or(phoneJoined.col("MAID_Two").equalTo(maidIndex.col("TU_maid")))
                        .or(phoneJoined.col("MAID_Three").equalTo(maidIndex.col("TU_maid"))),
                "left_outer");

        // Join on Address
        Dataset<Row> addressJoined = maidJoined.join(addressIndex,
                maidJoined.col("House_Number").equalTo(addressIndex.col("TU_houseNumber"))
                        .and(maidJoined.col("Street_Name").equalTo(addressIndex.col("TU_streetName")))
                        .and(maidJoined.col("ZIP_Code").equalTo(addressIndex.col("TU_Zip"))),
                "left_outer");

        return addressJoined;
    }

    private static Dataset<Row> filterByNameLevenshtein(Dataset<Row> data) {
        // Add Levenshtein distance for Client First_Name to Index TU_FirstName
        Dataset<Row> firstToFirstMatch = data.withColumn("levenshtein_score_first_first",
                functions.expr("1 - levenshtein(First_Name, TU_FirstName) / greatest(length(First_Name), length(TU_FirstName))"));

        // Add Levenshtein distance for Client Last_Name to Index TU_LastName
        Dataset<Row> lastToLastMatch = firstToFirstMatch.withColumn("levenshtein_score_last_last",
                functions.expr("1 - levenshtein(Last_Name, TU_LastName) / greatest(length(Last_Name), length(TU_LastName))"));

        // Add Levenshtein distance for Client Last_Name to Index TU_FirstName
        Dataset<Row> lastToFirstMatch = lastToLastMatch.withColumn("levenshtein_score_last_first",
                functions.expr("1 - levenshtein(Last_Name, TU_FirstName) / greatest(length(Last_Name), length(TU_FirstName))"));

        // Add Levenshtein distance for Client First_Name to Index TU_LastName
        Dataset<Row> firstToLastMatch = lastToFirstMatch.withColumn("levenshtein_score_first_last",
                functions.expr("1 - levenshtein(First_Name, TU_LastName) / greatest(length(First_Name), length(TU_LastName))"));

        // Filter rows based on the condition:
        // (levenshtein_score_first_first >= 0.8 AND levenshtein_score_last_last >= 0.8)
        // OR (levenshtein_score_last_first >= 0.8 AND levenshtein_score_first_last >= 0.8)
        Dataset<Row> filteredData = firstToLastMatch.filter(
                "(levenshtein_score_first_first >= 0.8 AND levenshtein_score_last_last >= 0.8) " +
                        "OR (levenshtein_score_last_first >= 0.8 AND levenshtein_score_first_last >= 0.8)"
        );

        return filteredData;
    }
}