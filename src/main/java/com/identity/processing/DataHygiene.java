package com.identity.processing;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

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
        
        // Step 6: Apply address hygiene rules
        Dataset<Row> addressHygieneApplied = applyAddressHygieneRules(dobHygieneApplied);

        // Step 7: Cleanse join columns
        Dataset<Row> cleansedData = cleanseJoinColumns(addressHygieneApplied);

        // Step 8: Derive Email type
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
                // Combine email fields into a single array, remove nulls, remove blanks, and deduplicate
    			// Add index for Email_Address_One, Two, and Three
    			// Add indices for elements in Email_Adress_Array starting from 4
                .withColumn("Email_Adress_Array_With_Index",
                        functions.expr(
                                "ARRAY_DISTINCT(FILTER(" +
                                        "TRANSFORM(" +
                                        "   array(named_struct('index', 1, 'email', Email_Address_One), " +
                                        "         named_struct('index', 2, 'email', Email_Address_Two), " +
                                        "         named_struct('index', 3, 'email', Email_Address_Three))" +
                                        "   || IFNULL(TRANSFORM(Email_Adress_Array, (e, i) -> named_struct('index', i + 4, 'email', e)), array())," +
                                        "   x -> x.email IS NOT NULL AND x.email != ''" +
                                        "), y -> y IS NOT NULL))"))
                .drop("Email_Address_One", "Email_Address_Two", "Email_Address_Three", "Email_Adress_Array")
                
                // Add index for Phone_Number_One, Two, and Three
    			// Add indices for elements in Phone_Number_Array starting from 4
                .withColumn("Phone_Number_Array_With_Index",
                        functions.expr(
                                "ARRAY_DISTINCT(FILTER(" +
                                        "TRANSFORM(" +
                                        "   array(named_struct('index', 1, 'phone', Phone_Number_One), " +
                                        "         named_struct('index', 2, 'phone', Phone_Number_Two), " +
                                        "         named_struct('index', 3, 'phone', Phone_Number_Three))" +
                                        "   || IFNULL(TRANSFORM(Phone_Number_Array, (e, i) -> named_struct('index', i + 4, 'phone', e)), array())," +
                                        "   x -> x.phone IS NOT NULL AND x.phone != ''" +
                                        "), y -> y IS NOT NULL))"))
                .drop("Phone_Number_One", "Phone_Number_Two", "Phone_Number_Three", "Phone_Number_Array")

                // Combine MAID fields into a single array, remove nulls, remove blanks, and deduplicate
                .withColumn("MAID_and_MAID_Type_Array",
                        functions.array_distinct(
                                functions.array_except(
                                        functions.array(data.col("MAID_One"),
                                                        data.col("MAID_Two"),
                                                        data.col("MAID_Three")),
                                        functions.lit(new String[]{null, ""}) // Remove null and empty string
                                )
                        )
                );
    }

    /**
     * Explodes arrays for PII fields into individual rows.
     *
     * @param data Dataset with deduplicated arrays.
     * @return Exploded dataset.
     */
    private static Dataset<Row> explodeArrayFields(Dataset<Row> data) {
    	
		// Explode Email_Array
		Dataset<Row> emailExploded = data
				// Explode Email_Adress_Array_With_Index into individual rows
				.withColumn("Exploded_Email", functions.explode_outer(data.col("Email_Adress_Array_With_Index")))
				// Extract index and email from struct
				.withColumn("Email_Index", functions.col("Exploded_Email.index"))
				.withColumn("Email_Address_One", functions.col("Exploded_Email.email"))
				.drop("Exploded_Email", "Email_Adress_Array_With_Index");
        
        // Explode Phone_Array
		Dataset<Row> phoneExploded = emailExploded
				// Explode Phone_Number_Array_With_Index into individual rows
				.withColumn("Exploded_Phone", functions.explode_outer(data.col("Phone_Number_Array_With_Index")))
				// Extract index and phone from struct
				.withColumn("Phone_Index", functions.col("Exploded_Phone.index"))
				.withColumn("Phone_Number_One", functions.col("Exploded_Phone.email"))
				.drop("Exploded_Phone", "Phone_Number_Array_With_Index");
		

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
        String cleanNameRegex = "[^a-zA-Z ]";
        String suffixRegex = "( Jr| Sr| II| III| IV| V)$";

        Map<String, Integer[]> nameFormatMapping = new HashMap<>();
        nameFormatMapping.put("F", new Integer[]{0, null, null});
        nameFormatMapping.put("FL", new Integer[]{0, null, 1});
        nameFormatMapping.put("FML", new Integer[]{0, 1, 2});
        // Add other mappings...

        Broadcast<Map<String, Integer[]>> broadcastMapping = SparkSession.active().sparkContext()
                .broadcast(nameFormatMapping, scala.reflect.ClassTag$.MODULE$.apply(Map.class));

        // Encoder for the resulting Dataset<Row>
        Encoder<Row> rowEncoder = Encoders.bean(Row.class);

        return data
                .withColumn("parsed_name_array", functions.when(
                        functions.col("Unparsed_Name").isNotNull(),
                        functions.split(functions.col("Unparsed_Name"), " ")
                ).otherwise(functions.lit(null)))
                .mapPartitions((MapPartitionsFunction<Row, Row>) rows -> {
                    Map<String, Integer[]> mapping = broadcastMapping.value();
                    List<Row> processedRows = new ArrayList<>();

                    while (rows.hasNext()) {
                        Row row = rows.next();
                        String format = row.getAs("Unparsed_Name_Format");
                        List<String> parsedArray = row.getList(row.fieldIndex("parsed_name_array"));
                        String firstName = null, middleName = null, lastName = null;

                        if (format != null && parsedArray != null && mapping.containsKey(format)) {
                            Integer[] indices = mapping.get(format);

                            // Assign names dynamically based on indices
                            if (indices.length >= 2) {
                                firstName = indices[0] != null && indices[0] < parsedArray.size() ? parsedArray.get(indices[0]) : null;
                                lastName = indices[2] != null && indices[2] < parsedArray.size() ? parsedArray.get(indices[2]) : null;
                            }
                            if (indices.length == 3) {
                                middleName = indices[1] != null && indices[1] < parsedArray.size() ? parsedArray.get(indices[1]) : null;
                            }
                        }

                        // Add cleaned names and suffix extraction
                        processedRows.add(RowFactory.create(
                                cleanName(firstName, cleanNameRegex),     // Cleaned First_Name
                                cleanName(middleName, cleanNameRegex),   // Cleaned Middle_Name
                                cleanName(lastName, cleanNameRegex),     // Cleaned Last_Name
                                extractSuffix(lastName, suffixRegex)     // Extracted Generational Suffix
                        ));
                    }
                    return processedRows.iterator();
                }, rowEncoder); // Specify the encoder to remove ambiguity
    }


    /**
     * Helper method to clean names by removing invalid characters.
     *
     * @param name  The input name string.
     * @param regex The regex for removing unwanted characters.
     * @return The cleaned name string.
     */
    private static String cleanName(String name, String regex) {
        return name == null ? null : name.replaceAll(regex, "").trim();
    }

    /**
     * Helper method to extract generational suffix from a name.
     *
     * @param lastName The input last name string.
     * @param regex    The regex for extracting the suffix.
     * @return The extracted generational suffix, or null if not found.
     */
    private static String extractSuffix(String lastName, String regex) {
        if (lastName == null) {
            return null;
        }
        Matcher matcher = Pattern.compile(regex).matcher(lastName);
        return matcher.find() ? matcher.group(1) : null;
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
    
    /**
     * Applies address hygiene rules to clean and normalize address-related fields.
     *
     * @param data Input dataset.
     * @return Dataset with cleansed address fields.
     */
    private static Dataset<Row> applyAddressHygieneRules(Dataset<Row> data) {
        // Predefine reusable patterns for regex replacements
        String nonAlphanumericStreetPattern = "[^a-zA-Z0-9 #/\\-]";
        String repeatedSpecialCharsPattern = "(\\s|[#/\\-]){2,}";
        String nonAlphaCityPattern = "[^a-zA-Z \\-]";
        String repeatedCitySpecialCharsPattern = "(\\s|\\-){2,}";

        // Define static list of valid US state codes
        String[] validStateCodes = {
                "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
                "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
                "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
                "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
                "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
        };

        return data
                // Process Street_Name
                .withColumn("Street_Name", functions.expr(
                        "CASE WHEN Street_Name IS NOT NULL THEN " +
                                "CASE WHEN Street_Name rlike '[a-zA-Z0-9]' THEN " +
                                "regexp_replace(regexp_replace(trim(Street_Name), '" + nonAlphanumericStreetPattern + "', ''), '" + repeatedSpecialCharsPattern + "', ' ') " +
                                "ELSE NULL END " +
                                "ELSE NULL END"
                ))

                // Process Street_Address_Line_1
                .withColumn("Street_Address_Line_1", functions.expr(
                        "CASE WHEN Street_Address_Line_1 IS NOT NULL THEN " +
                                "CASE WHEN Street_Address_Line_1 rlike '[a-zA-Z0-9]' THEN " +
                                "regexp_replace(regexp_replace(trim(Street_Address_Line_1), '" + nonAlphanumericStreetPattern + "', ''), '\\s{2,}', ' ') " +
                                "ELSE NULL END " +
                                "ELSE NULL END"
                ))

                // Process House_Number
                .withColumn("House_Number", functions.expr(
                        "CASE WHEN House_Number IS NOT NULL THEN " +
                                "CASE WHEN House_Number rlike '[a-zA-Z0-9]' THEN " +
                                "regexp_replace(regexp_replace(trim(House_Number), '" + nonAlphanumericStreetPattern + "', ''), '" + repeatedSpecialCharsPattern + "', ' ') " +
                                "ELSE NULL END " +
                                "ELSE NULL END"
                ))

                // Process Unit_Number
                .withColumn("Unit_Number", functions.expr(
                        "CASE WHEN Unit_Number IS NOT NULL THEN " +
                                "CASE WHEN Unit_Number rlike '[a-zA-Z0-9]' THEN regexp_replace(trim(Unit_Number), '[^a-zA-Z0-9]', '') " +
                                "ELSE NULL END " +
                                "ELSE NULL END"
                ))

                // Process City
                .withColumn("City", functions.expr(
                        "CASE WHEN City IS NOT NULL THEN " +
                                "CASE WHEN City rlike '[a-zA-Z]' THEN " +
                                "regexp_replace(regexp_replace(trim(City), '" + nonAlphaCityPattern + "', ''), '" + repeatedCitySpecialCharsPattern + "', ' ') " +
                                "ELSE NULL END " +
                                "ELSE NULL END"
                ))

                // Process State with static valid state codes
                .withColumn("State", functions.when(
                        functions.array_contains(functions.lit(validStateCodes), functions.col("State")),
                        functions.col("State")
                ).otherwise((String) null))

                // Process Street_Type, Unit_Type, Post_Directional, Pre_Directional
                .withColumn("Street_Type", functions.expr(
                        "CASE WHEN Street_Type IS NOT NULL THEN regexp_replace(trim(Street_Type), '[^a-zA-Z]', '') ELSE NULL END"
                ))
                .withColumn("Unit_Type", functions.expr(
                        "CASE WHEN Unit_Type IS NOT NULL THEN regexp_replace(trim(Unit_Type), '[^a-zA-Z]', '') ELSE NULL END"
                ))
                .withColumn("Post_Directional", functions.expr(
                        "CASE WHEN Post_Directional IS NOT NULL THEN regexp_replace(trim(Post_Directional), '[^a-zA-Z]', '') ELSE NULL END"
                ))
                .withColumn("Pre_Directional", functions.expr(
                        "CASE WHEN Pre_Directional IS NOT NULL THEN regexp_replace(trim(Pre_Directional), '[^a-zA-Z]', '') ELSE NULL END"
                ));
    }

}
