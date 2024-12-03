package com.identity.processing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.time.LocalDate;

/**
 * Cleans, normalizes, deduplicates, and explodes PII fields in the dataset.
 * Generates a unique ID for each row after processing.
 */
public class DataHygiene {

    /**
     * Cleanses the input data based on hygiene rules, deduplicates PII fields, explodes arrays, and generates unique IDs.
     *
     * @param data Raw client dataset.
     * @return Cleaned, exploded, and uniquely identified dataset.
     */
    public static Dataset<Row> cleanAndExplodeData(Dataset<Row> data) {
        // Step 1: Consolidate and deduplicate PII fields
        Dataset<Row> consolidatedData = consolidateAndDeduplicatePIIFields(data);

        // Step 2: Explode consolidated arrays
        Dataset<Row> explodedData = explodeArrayFields(consolidatedData);

        // Step 3: Generate a unique ID for each row
        Dataset<Row> withUniqueId = generateUniqueId(explodedData);

        // Step 4: Apply name hygiene rules
        Dataset<Row> nameHygieneApplied = applyNameHygieneRules(withUniqueId);

        // Step 5: Apply DOB hygiene rules
        Dataset<Row> dobHygieneApplied = applyDOBHygieneRules(nameHygieneApplied);

        // Step 6: Cleanse join columns
        Dataset<Row> cleansedData = cleanseJoinColumns(dobHygieneApplied);

        // Step 7: Derive Email type
        Dataset<Row> result = deriveEmailType(cleansedData);

        return result;
    }

    /**
     * Consolidates and deduplicates PII fields (Emails, Phones, MAIDs).
     *
     * @param data Input dataset.
     * @return Dataset with consolidated and deduplicated arrays for PII fields.
     */
    private static Dataset<Row> consolidateAndDeduplicatePIIFields(Dataset<Row> data) {
        return data
                // Combine email fields into a single array and deduplicate
                .withColumn("Email_Array",
                        functions.array_distinct(
                                functions.array(
                                        data.col("Email_Address_One"),
                                        data.col("Email_Address_Two"),
                                        data.col("Email_Address_Three")
                                )
                        ))

                // Combine phone fields into a single array and deduplicate
                .withColumn("Phone_Array",
                        functions.array_distinct(
                                functions.array(
                                        data.col("Phone_Number_One"),
                                        data.col("Phone_Number_Two"),
                                        data.col("Phone_Number_Three")
                                )
                        ))

                // Combine MAID fields into a single array and deduplicate
                .withColumn("MAID_Array",
                        functions.array_distinct(
                                functions.array(
                                        data.col("MAID_One"),
                                        data.col("MAID_Two"),
                                        data.col("MAID_Three")
                                )
                        ));
    }

    /**
     * Explodes arrays for PII fields into individual rows.
     *
     * @param data Dataset with deduplicated arrays.
     * @return Exploded dataset.
     */
    private static Dataset<Row> explodeArrayFields(Dataset<Row> data) {
        // Explode Email_Array
        Dataset<Row> emailExploded = data.withColumn("Email_Exploded",
                functions.explode_outer(data.col("Email_Array")))
                .withColumn("Email_Address_One", functions.col("Email_Exploded"))
                .drop("Email_Exploded");

        // Explode Phone_Array
        Dataset<Row> phoneExploded = emailExploded.withColumn("Phone_Exploded",
                functions.explode_outer(emailExploded.col("Phone_Array")))
                .withColumn("Phone_Number_One", functions.col("Phone_Exploded"))
                .drop("Phone_Exploded");

        // Explode MAID_Array
        Dataset<Row> maidExploded = phoneExploded.withColumn("MAID_Exploded",
                functions.explode_outer(phoneExploded.col("MAID_Array")))
                .withColumn("MAID_One", functions.col("MAID_Exploded"))
                .drop("MAID_Exploded");

        return maidExploded;
    }

    /**
     * Generates a unique ID for each row using the UUID function.
     *
     * @param data Dataset after explosion.
     * @return Dataset with a unique ID for each row.
     */
    private static Dataset<Row> generateUniqueId(Dataset<Row> data) {
        return data.withColumn("Generated_Record_ID", functions.expr("uuid()"));
    }

    private static Dataset<Row> applyNameHygieneRules(Dataset<Row> data) {
        return data
                .withColumn("First_Name", functions.trim(functions.regexp_replace(functions.col("First_Name"), "[^a-zA-Z ]", "")))
                .withColumn("Middle_Name", functions.trim(functions.regexp_replace(functions.col("Middle_Name"), "[^a-zA-Z ]", "")))
                .withColumn("Last_Name", functions.trim(functions.expr("regexp_replace(Last_Name, '( Jr| Sr| II| III| IV| V)$', '')")))
                .withColumn("Gen_Suffix", functions.expr(
                        "CASE WHEN Last_Name rlike '( Jr| Sr| II| III| IV| V)$' THEN regexp_extract(Last_Name, '( Jr| Sr| II| III| IV| V)$', 1) ELSE NULL END"
                ));
    }

    private static Dataset<Row> applyDOBHygieneRules(Dataset<Row> data) {
        LocalDate currentDate = LocalDate.now();

        return data
                .withColumn("DOB", functions.expr(
                        "CASE " +
                                "WHEN Unparsed_Date_of_Birth rlike '^[0-9]{4}/[0-9]{2}/[0-9]{2}$' THEN to_date(Unparsed_Date_of_Birth, 'yyyy/MM/dd') " +
                                "WHEN Unparsed_Date_of_Birth rlike '^[0-9]{8}$' THEN to_date(Unparsed_Date_of_Birth, 'yyyyMMdd') " +
                                "WHEN Unparsed_Date_of_Birth rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN to_date(Unparsed_Date_of_Birth, 'yyyy-MM-dd') " +
                                "ELSE NULL END"
                ))
                .withColumn("Birth_Year", functions.expr(
                        "CASE WHEN Birth_Year rlike '^[0-9]+$' AND Birth_Year >= 1900 AND Birth_Year <= " + currentDate.getYear() + " THEN Birth_Year ELSE NULL END"
                ))
                .withColumn("Birth_Month", functions.expr(
                        "CASE WHEN Birth_Month rlike '^[0-9]+$' AND Birth_Month >= 1 AND Birth_Month <= 12 THEN lpad(Birth_Month, 2, '0') ELSE NULL END"
                ))
                .withColumn("Birth_Day", functions.expr(
                        "CASE WHEN Birth_Day rlike '^[0-9]+$' AND Birth_Day >= 1 AND Birth_Day <= 31 THEN lpad(Birth_Day, 2, '0') ELSE NULL END"
                ));
    }

    private static Dataset<Row> deriveEmailType(Dataset<Row> data) {
        return data.withColumn("email_type",
                functions.when(functions.col("Email_Address_One").contains("@")
                                .and(functions.col("Email_Address_One").contains(".")),
                        "CLEAR_TEXT")
                        .when(functions.length(functions.col("Email_Address_One")).equalTo(32), "MD5")
                        .when(functions.length(functions.col("Email_Address_One")).equalTo(40), "SHA1")
                        .when(functions.length(functions.col("Email_Address_One")).equalTo(64), "SHA256")
                        .otherwise(null));
    }

    private static Dataset<Row> cleanseJoinColumns(Dataset<Row> data) {
        return data
                .withColumn("Email_Address_One", functions.trim(data.col("Email_Address_One")))
                .withColumn("Phone_Number_One", functions.regexp_replace(data.col("Phone_Number_One"), "[^0-9]", ""))
                .withColumn("MAID_One", functions.trim(data.col("MAID_One")))
                .withColumn("ZIP_Code", functions.regexp_replace(data.col("ZIP_Code"), "[^0-9]", ""));
    }
}
