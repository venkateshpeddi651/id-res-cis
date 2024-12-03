package com.identity.processing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static com.identity.processing.NameMatchKeyRankMap.matchKeyRankMap;
import static com.identity.processing.constants.IndexKeys.*;

import java.util.Map;

/**
 * Implements identity matching logic using Spark DataFrames.
 * Handles joins based on index definitions and filters using Levenshtein distances for approximate name matching.
 */
public class IdentityMatcher {

    public static Dataset<Row> performMatching(Dataset<Row> clientData,
                                               Map<String, Dataset<Row>> indexTables) {
        // Step 1: Name Dob Index 
        Dataset<Row> nameDob = nameDoBIndex(clientData, indexTables.get(PARTIAL_DOB_INDEX), 
        									indexTables.get(FULL_DOB_INDEX),
        									indexTables.get(NAME_INDEX));

        // Step 2: Name Email Index 
        Dataset<Row> nameEmail = nameEmailIndex(clientData, indexTables.get(CLEAR_TEXT_EMAIL_INDEX), 
											indexTables.get(MD5_EMAIL_INDEX),
											indexTables.get(SHA1_EMAIL_INDEX),
											indexTables.get(SHA256_EMAIL_INDEX),
											indexTables.get(NAME_INDEX));

        return Utils.unionDatasets(nameDob, nameEmail);
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
    	Dataset<Row> finalDobResult = findBestDobCluster(nameRanks);
    	
    	// Step 4: Add name_dob_hit_ind 
        return addHitIndicator(finalDobResult, "name_dob_hit_ind");
    }
    
    private static Dataset<Row> nameEmailIndex(Dataset<Row> clientData,
									            Dataset<Row> clearTextEmailIndex,
									            Dataset<Row> md5EmailIndex,
									            Dataset<Row> sha1EmailIndex,
									            Dataset<Row> sha256EmailIndex,
									            Dataset<Row> nameIndex) {
    	// Step1 : 4 joins
    	Dataset<Row> emailJoins = performEmailJoins(clientData, clearTextEmailIndex, md5EmailIndex, sha1EmailIndex, sha256EmailIndex);
    	
    	// Step2: perform name matching and assign name rank
    	Dataset<Row> nameRanks = assignNameRank(emailJoins, nameIndex);
    	
    	// Step3: Find best cluster based on name_rank & join_rank
    	Dataset<Row> finalEmailResult = findBestEmailCluster(nameRanks);
    	
    	// Step 4: Add name_email_hit_ind 
        return addHitIndicator(finalEmailResult, "name_email_hit_ind");
    }
    
    private static Dataset<Row> addHitIndicator(Dataset<Row> data, String hitIndicatorColumn) {
        // Add hit indicator
        Dataset<Row> withHitIndicator = data.withColumn(hitIndicatorColumn,
                functions.when(functions.col("clusterId").isNotNull().and(functions.col("clusterId").notEqual("")), "Y")
                        .otherwise("N"));

        // Wipe out clusterId and set hit indicator to 'HR' for name_rank > 50
        return withHitIndicator
                .withColumn("clusterId", functions.when(functions.col("name_rank").leq(50), functions.col("clusterId")).otherwise(null))
                .withColumn(hitIndicatorColumn, functions.when(functions.col("name_rank").leq(50), functions.col(hitIndicatorColumn)).otherwise("HR"));
    }
    
	private static Dataset<Row> performEmailJoins(Dataset<Row> hygieneData, Dataset<Row> emailCleartextIndex,
			Dataset<Row> emailMd5Index, Dataset<Row> emailSha1Index, Dataset<Row> emailSha256Index) {
		Dataset<Row> cleartextJoin = hygieneData.filter(
				"Email_Address_One IS NOT NULL AND First_Name IS NOT NULL AND Last_Name IS NOT NULL AND email_type = 'CLEAR_TEXT'")
				.join(emailCleartextIndex,
						hygieneData.col("Email_Address_One").equalTo(emailCleartextIndex.col("TU_email")), "inner");

		Dataset<Row> md5Join = hygieneData.filter(
				"Email_Address_One IS NOT NULL AND First_Name IS NOT NULL AND Last_Name IS NOT NULL AND email_type = 'MD5'")
				.join(emailMd5Index, hygieneData.col("Email_Address_One").equalTo(emailMd5Index.col("TU_email")),
						"inner");

		Dataset<Row> sha1Join = hygieneData.filter(
				"Email_Address_One IS NOT NULL AND First_Name IS NOT NULL AND Last_Name IS NOT NULL AND email_type = 'SHA1'")
				.join(emailSha1Index, hygieneData.col("Email_Address_One").equalTo(emailSha1Index.col("TU_email")),
						"inner");

		Dataset<Row> sha256Join = hygieneData.filter(
				"Email_Address_One IS NOT NULL AND First_Name IS NOT NULL AND Last_Name IS NOT NULL AND email_type = 'SHA256'")
				.join(emailSha256Index, hygieneData.col("Email_Address_One").equalTo(emailSha256Index.col("TU_email")),
						"inner");

		return cleartextJoin.union(md5Join).union(sha1Join).union(sha256Join);
	}

	private static Dataset<Row> findBestDobCluster(Dataset<Row> nameRanks) {
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
	
	private static Dataset<Row> findBestEmailCluster(Dataset<Row> nameRanks) {
		// Define the window partitioned by clusterId and ordered by rank
	    WindowSpec windowSpec = Window.partitionBy("clusterId")
	                                   .orderBy(functions.col("name_rank").asc());

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
	
}