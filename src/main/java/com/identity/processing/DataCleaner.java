package com.identity.processing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Cleans, normalizes, and explodes data, generating unique IDs for each row.
 */
public class DataCleaner {

    public static Dataset<Row> cleanAndExplodeData(Dataset<Row> data) {
        // Step 1: Trim leading/trailing spaces and clean basic PII fields
        data = cleanData(data);

        // Step 2: Explode array fields into multiple rows
        data = explodeArrayFields(data);

        // Step 3: Generate a unique ID for each row
        data = generateUniqueId(data);

        return data;
    }

    private static Dataset<Row> cleanData(Dataset<Row> data) {
        // Example: Trim spaces and apply hygiene rules to names
        return data.withColumn("First_Name", functions.trim(data.col("First_Name")))
                .withColumn("Last_Name", functions.trim(data.col("Last_Name")))
                .withColumn("Middle_Name", functions.trim(data.col("Middle_Name")));
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

    private static Dataset<Row> generateUniqueId(Dataset<Row> data) {
        // Add a unique ID column for each row
        return data.withColumn("Unique_ID", functions.expr("uuid()"));
    }
}