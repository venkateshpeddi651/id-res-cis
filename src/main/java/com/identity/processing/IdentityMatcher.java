package com.identity.processing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static com.identity.processing.NameMatchKeyRankMap.matchKeyRankMap;

/**
 * Implements identity matching logic using Spark DataFrames.
 * Handles joins based on index definitions and filters using Levenshtein distances for approximate name matching.
 */
public class IdentityMatcher {

    public static Dataset<Row> performMatching(Dataset<Row> clientData,
                                               Dataset<Row> partialDobIndex,
                                               Dataset<Row> fullDobIndex,
                                               Dataset<Row> nameIndex) {
        // Step 1: Name Dob Index 
        Dataset<Row> nameDob = nameDoBIndex(clientData, partialDobIndex, fullDobIndex, nameIndex);

        

        return nameDob;
    }

    private static Dataset<Row> nameDoBIndex(Dataset<Row> clientData,
                                                    Dataset<Row> partialDobIndex,
                                                    Dataset<Row> fullDobIndex,
                                                    Dataset<Row> nameIndex) {
    	
    	// Step1 : 5 joins
    	Dataset<Row> dobJoins = performDoBJoins(clientData, partialDobIndex, fullDobIndex);
  
    	// Step2: perform name matching and assign name rank
    	Dataset<Row> nameRanks = assignNameRank(dobJoins, nameIndex);
    	
    	// Step3: Find best cluster based on name_rank & join_rank
    	Dataset<Row> finalDobResult = findBestCluster(nameRanks);
    	
    	return finalDobResult;
    }

	private static Dataset<Row> findBestCluster(Dataset<Row> nameRanks) {
		// Define the window partitioned by clusterId and ordered by rank
	    WindowSpec windowSpec = Window.partitionBy("clusterId")
	                                   .orderBy(functions.col("name_rank").asc(), 
	                                            functions.col("name_dob_join_rank").asc());

	    // Add a rank column to identify the best record for each clusterId
	    Dataset<Row> rankedData = nameRanks.withColumn("rank", functions.row_number().over(windowSpec));

	    // Filter only the best-ranked records
	    Dataset<Row> result = rankedData.filter("rank = 1").drop("rank");

	    return result;
	}

	private static Dataset<Row> performDoBJoins(Dataset<Row> clientData, Dataset<Row> partialDobIndex,
			Dataset<Row> fullDobIndex) {
    	// Join 1
        Dataset<Row> join1 = clientData
                .filter("ZIP_Code IS NOT NULL AND Birth_Year IS NOT NULL AND First_Name IS NOT NULL AND Last_Name IS NOT NULL")
                .join(partialDobIndex,
                        clientData.col("ZIP_Code").equalTo(partialDobIndex.col("TU_zip"))
                                .and(clientData.col("Birth_Year").equalTo(partialDobIndex.col("TU_DoB_YYYY")))
                                .and(functions.substring(clientData.col("First_Name"), 1, 1).equalTo(partialDobIndex.col("TU_finit")))
                                .and(functions.substring(clientData.col("Last_Name"), 1, 1).equalTo(partialDobIndex.col("TU_linit"))),
                        "inner")
                .withColumn("name_dob_join_rank", functions.lit(1));

        // Join 2
        Dataset<Row> join2 = clientData
                .filter("ZIP_Code IS NOT NULL AND Birth_Month IS NOT NULL AND Birth_Day IS NOT NULL AND First_Name IS NOT NULL AND Last_Name IS NOT NULL")
                .join(partialDobIndex,
                        clientData.col("ZIP_Code").equalTo(partialDobIndex.col("TU_zip"))
                                .and(clientData.col("Birth_Month").equalTo(partialDobIndex.col("TU_DoB_MM")))
                                .and(clientData.col("Birth_Day").equalTo(partialDobIndex.col("TU_DoB_DD")))
                                .and(functions.substring(clientData.col("First_Name"), 1, 1).equalTo(partialDobIndex.col("TU_finit")))
                                .and(functions.substring(clientData.col("Last_Name"), 1, 1).equalTo(partialDobIndex.col("TU_linit"))),
                        "inner")
                .withColumn("name_dob_join_rank", functions.lit(2));

        // Join 3
        Dataset<Row> join3 = clientData
                .filter("City IS NOT NULL AND State IS NOT NULL AND Birth_Year IS NOT NULL AND First_Name IS NOT NULL AND Last_Name IS NOT NULL")
                .join(partialDobIndex,
                        clientData.col("City").equalTo(partialDobIndex.col("TU_city"))
                                .and(clientData.col("State").equalTo(partialDobIndex.col("TU_state")))
                                .and(clientData.col("Birth_Year").equalTo(partialDobIndex.col("TU_DoB_YYYY")))
                                .and(functions.substring(clientData.col("First_Name"), 1, 1).equalTo(partialDobIndex.col("TU_finit")))
                                .and(functions.substring(clientData.col("Last_Name"), 1, 1).equalTo(partialDobIndex.col("TU_linit"))),
                        "inner")
                .withColumn("name_dob_join_rank", functions.lit(3));

        // Join 4
        Dataset<Row> join4 = clientData
                .filter("City IS NOT NULL AND State IS NOT NULL AND Birth_Month IS NOT NULL AND Birth_Day IS NOT NULL AND First_Name IS NOT NULL AND Last_Name IS NOT NULL")
                .join(partialDobIndex,
                        clientData.col("City").equalTo(partialDobIndex.col("TU_city"))
                                .and(clientData.col("State").equalTo(partialDobIndex.col("TU_state")))
                                .and(clientData.col("Birth_Month").equalTo(partialDobIndex.col("TU_DoB_MM")))
                                .and(clientData.col("Birth_Day").equalTo(partialDobIndex.col("TU_DoB_DD")))
                                .and(functions.substring(clientData.col("First_Name"), 1, 1).equalTo(partialDobIndex.col("TU_finit")))
                                .and(functions.substring(clientData.col("Last_Name"), 1, 1).equalTo(partialDobIndex.col("TU_linit"))),
                        "inner")
                .withColumn("name_dob_join_rank", functions.lit(4));


        Dataset<Row> join5 = clientData
                .filter("Birth_Year IS NOT NULL AND Birth_Month IS NOT NULL AND Birth_Day IS NOT NULL AND First_Name IS NOT NULL AND Last_Name IS NOT NULL")
                .join(fullDobIndex,
                        functions.concat(clientData.col("Birth_Year"), clientData.col("Birth_Month"), clientData.col("Birth_Day"))
                                .equalTo(fullDobIndex.col("TU_DoB"))
                                .and(functions.substring(clientData.col("First_Name"), 1, 1).equalTo(fullDobIndex.col("TU_finit")))
                                .and(functions.substring(clientData.col("Last_Name"), 1, 1).equalTo(fullDobIndex.col("TU_linit"))),
                        "inner")
                .withColumn("name_dob_join_rank", functions.lit(5));

        // Union all joins
        return join1.union(join2).union(join3).union(join4).union(join5);
	}

	private static Dataset<Row> assignNameRank(Dataset<Row> dobJoins, Dataset<Row> nameIndex) {
		// Join with name_matching_index
        Dataset<Row> nameJoined = dobJoins.join(nameIndex, "clusterId");

        // Add NameComparisonUDF
        Dataset<Row> withMatchKey = nameJoined.withColumn("MATCHKEY",
                functions.callUDF("NameComparisonUDF",
                        nameJoined.col("First_Name"), nameJoined.col("preferred_first_name"),
                        functions.substring(nameJoined.col("First_Name"), 1, 1),
                        nameJoined.col("Middle_Name"), functions.substring(nameJoined.col("Middle_Name"), 1, 1),
                        nameJoined.col("Last_Name"), functions.substring(nameJoined.col("Last_Name"), 1, 1),
                        nameJoined.col("Generational_Suffix"),
                        nameJoined.col("TU_firstname"), nameJoined.col("TU_pref_firstname"),
                        nameJoined.col("TU_lastname"), nameJoined.col("TU_middlename"), nameJoined.col("TU_gensuffix"),
                        nameJoined.col("first_name_sndx"), nameJoined.col("last_name_sndx"),
                        nameJoined.col("TU_sndx_firstname"), nameJoined.col("TU_sndx_lastname")));

        
       // Add name_rank using a case expression built from the map
       return withMatchKey.withColumn("name_rank",
                functions.expr(matchKeyRankMap.entrySet().stream()
                        .map(entry -> "WHEN MATCHKEY = '" + entry.getKey() + "' THEN " + entry.getValue())
                        .reduce((a, b) -> a + " " + b)
                        .orElse("ELSE 100")));
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