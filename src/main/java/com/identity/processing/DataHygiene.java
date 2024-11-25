package com.identity.processing;

import java.time.LocalDate;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Cleans, normalizes, and explodes data. Generates a unique ID for each row after explosion.
 */
public class DataHygiene {

    /**
     * Cleanses the input data based on hygiene rules, explodes arrays, and generates unique IDs.
     *
     * @param data Raw client dataset.
     * @return Cleaned, exploded, and uniquely identified dataset.
     */
    public static Dataset<Row> cleanAndExplodeData(Dataset<Row> data) {
        // Step 1: Explode array fields
        Dataset<Row> explodedData = explodeArrayFields(data);

        // Step 2: Generate a unique ID for each row
        Dataset<Row> withUniqueId = generateUniqueId(explodedData);

        // Step 3: Apply name hygiene rules
        Dataset<Row> nameHygieneApplied = applyNameHygieneRules(withUniqueId);

        // Step 4: Apply DOB hygiene rules
        Dataset<Row> dobHygieneApplied = applyDOBHygieneRules(nameHygieneApplied);

        // Step 5: Cleanse join columns
        Dataset<Row> cleansedData = cleanseJoinColumns(dobHygieneApplied);

        return cleansedData;
    }

    private static Dataset<Row> explodeArrayFields(Dataset<Row> data) {
        // Explode Email_Address_Array
        Dataset<Row> emailExploded = data.withColumn("Email_Exploded",
                functions.explode_outer(data.col("Email_Address_Array")))
                .withColumn("Email_Address_One", functions.col("Email_Exploded"))
                .drop("Email_Exploded");

        // Explode Phone_Number_Array
        Dataset<Row> phoneExploded = emailExploded.withColumn("Phone_Exploded",
                functions.explode_outer(emailExploded.col("Phone_Number_Array")))
                .withColumn("Phone_Number_One", functions.col("Phone_Exploded"))
                .drop("Phone_Exploded");

        // Explode MAID_and_MAID_Type_Array
        Dataset<Row> maidExploded = phoneExploded.withColumn("MAID_Exploded",
                functions.explode_outer(phoneExploded.col("MAID_and_MAID_Type_Array")))
                .withColumn("MAID_One", functions.split(functions.col("MAID_Exploded"), "\\|").getItem(0))
                .withColumn("MAID_Device_Type_One", functions.split(functions.col("MAID_Exploded"), "\\|").getItem(1))
                .drop("MAID_Exploded");

        // Explode Address_Array
        Dataset<Row> addressExploded = maidExploded.withColumn("Address_Exploded",
                functions.explode_outer(maidExploded.col("Address_Array")))
                .withColumn("Exploded_Address_Parts", functions.split(functions.col("Address_Exploded"), "\\|"))
                .withColumn("House_Number", functions.col("Exploded_Address_Parts").getItem(0))
                .withColumn("Street_Name", functions.col("Exploded_Address_Parts").getItem(1))
                .withColumn("City", functions.col("Exploded_Address_Parts").getItem(2))
                .withColumn("State", functions.col("Exploded_Address_Parts").getItem(3))
                .withColumn("ZIP_Code", functions.col("Exploded_Address_Parts").getItem(4))
                .drop("Address_Exploded", "Exploded_Address_Parts");

        return addressExploded;
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
                // Clean First_Name
                .withColumn("First_Name", functions.regexp_replace(functions.col("First_Name"), "[^a-zA-Z ]", ""))
                .withColumn("First_Name", functions.regexp_replace(functions.col("First_Name"), "\\s+", " "))
                .withColumn("First_Name", functions.trim(functions.col("First_Name")))

                // Clean Middle_Name
                .withColumn("Middle_Name", functions.regexp_replace(functions.col("Middle_Name"), "[^a-zA-Z ]", ""))
                .withColumn("Middle_Name", functions.regexp_replace(functions.col("Middle_Name"), "\\s+", " "))
                .withColumn("Middle_Name", functions.trim(functions.col("Middle_Name")))

                // Clean Last_Name and handle generational suffix
                .withColumn("Last_Name", functions.regexp_replace(functions.col("Last_Name"), "[^a-zA-Z ]", ""))
                .withColumn("Last_Name", functions.regexp_replace(functions.col("Last_Name"), "\\s+", " "))
                .withColumn("Last_Name", functions.trim(functions.expr(
                        "regexp_replace(Last_Name, '( Jr| Sr| II| III| IV| V)$', '')"
                )))

                // Extract generational suffix
                .withColumn("Gen_Suffix", functions.expr(
                        "CASE WHEN Last_Name rlike '( Jr| Sr| II| III| IV| V)$' THEN " +
                                "regexp_extract(Last_Name, '( Jr| Sr| II| III| IV| V)$', 1) ELSE NULL END"
                ));
    }

    private static Dataset<Row> applyDOBHygieneRules(Dataset<Row> data) {
        LocalDate currentDate = LocalDate.now();

        return data
                // Parse unparsed DOB into yyyy-MM-dd format
                .withColumn("DOB", functions.expr(
                        "CASE " +
                                "WHEN Unparsed_Date_of_Birth rlike '^[0-9]{4}/[0-9]{2}/[0-9]{2}$' THEN to_date(Unparsed_Date_of_Birth, 'yyyy/MM/dd') " +
                                "WHEN Unparsed_Date_of_Birth rlike '^[0-9]{8}$' THEN to_date(Unparsed_Date_of_Birth, 'yyyyMMdd') " +
                                "WHEN Unparsed_Date_of_Birth rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN to_date(Unparsed_Date_of_Birth, 'yyyy-MM-dd') " +
                                "ELSE NULL END"
                ))

                // Validate and normalize year
                .withColumn("Birth_Year", functions.expr(
                        "CASE WHEN Birth_Year rlike '^[0-9]+$' AND Birth_Year >= 1900 AND Birth_Year <= " + currentDate.getYear() + " " +
                                "THEN Birth_Year ELSE NULL END"
                ))

                // Validate and normalize month
                .withColumn("Birth_Month", functions.expr(
                        "CASE WHEN Birth_Month rlike '^[0-9]+$' AND Birth_Month >= 1 AND Birth_Month <= 12 " +
                                "THEN lpad(Birth_Month, 2, '0') ELSE NULL END"
                ))

                // Validate and normalize day
                .withColumn("Birth_Day", functions.expr(
                        "CASE WHEN Birth_Day rlike '^[0-9]+$' AND Birth_Day >= 1 AND Birth_Day <= 31 " +
                                "THEN lpad(Birth_Day, 2, '0') ELSE NULL END"
                ));
    }

    private static Dataset<Row> cleanseJoinColumns(Dataset<Row> data) {
        return data
                // Trim and normalize email columns
                .withColumn("Email_Address_One", functions.trim(data.col("Email_Address_One")))
                .withColumn("Email_Address_Two", functions.trim(data.col("Email_Address_Two")))
                .withColumn("Email_Address_Three", functions.trim(data.col("Email_Address_Three")))

                // Trim and normalize phone columns
                .withColumn("Phone_Number_One", functions.regexp_replace(data.col("Phone_Number_One"), "[^0-9]", ""))
                .withColumn("Phone_Number_Two", functions.regexp_replace(data.col("Phone_Number_Two"), "[^0-9]", ""))
                .withColumn("Phone_Number_Three", functions.regexp_replace(data.col("Phone_Number_Three"), "[^0-9]", ""))

                // Trim and normalize MAID columns
                .withColumn("MAID_One", functions.trim(data.col("MAID_One")))
                .withColumn("MAID_Two", functions.trim(data.col("MAID_Two")))
                .withColumn("MAID_Three", functions.trim(data.col("MAID_Three")))

                // Trim and normalize address columns
                .withColumn("House_Number", functions.trim(data.col("House_Number")))
                .withColumn("Street_Name", functions.trim(data.col("Street_Name")))
                .withColumn("City", functions.trim(data.col("City")))
                .withColumn("State", functions.trim(data.col("State")))
                .withColumn("ZIP_Code", functions.regexp_replace(data.col("ZIP_Code"), "[^0-9]", ""));
    }
}
