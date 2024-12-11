package com.identity.processing;

import java.util.Set;
import java.math.BigInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Utils {

    public static double levenshteinDistance(String s1, String s2) {
        // Implement Levenshtein distance calculation here
        return 0.0;
    }
    
    /**
     * Unions multiple datasets with potentially different schemas.
     * Dynamically aligns the schemas by adding missing columns as nulls and reordering columns.
     *
     * @param datasets Varargs of Dataset<Row> to union
     * @return Unified Dataset<Row>
     */
    @SafeVarargs
	public static Dataset<Row> unionDatasets(Dataset<Row>... datasets) {
        if (datasets == null || datasets.length == 0) {
            throw new IllegalArgumentException("No datasets provided for union");
        }

        // Collect all column names from all datasets
        Set<String> allColumns = Stream.of(datasets)
                .flatMap(dataset -> Stream.of(dataset.columns()))
                .collect(Collectors.toSet());

        // Align schemas for all datasets
        Dataset<Row> unifiedDataset = Stream.of(datasets)
                .map(dataset -> alignSchema(dataset, allColumns))
                .reduce(Utils::unionTwoDatasets)
                .orElseThrow(() -> new IllegalStateException("Failed to union datasets"));

        return unifiedDataset;
    }

    /**
     * Aligns a dataset's schema to include all columns, filling missing columns with nulls.
     *
     * @param dataset    The dataset to align
     * @param allColumns The complete set of column names
     * @return Dataset with aligned schema
     */
    private static Dataset<Row> alignSchema(Dataset<Row> dataset, Set<String> allColumns) {
        for (String column : allColumns) {
            if (!Stream.of(dataset.columns()).collect(Collectors.toSet()).contains(column)) {
                dataset = dataset.withColumn(column, functions.lit(null));
            }
        }
        return dataset.select(allColumns.stream().map(functions::col).toArray(org.apache.spark.sql.Column[]::new));
    }

    /**
     * Helper to union two datasets.
     *
     * @param dataset1 First dataset
     * @param dataset2 Second dataset
     * @return Unified Dataset<Row>
     */
    private static Dataset<Row> unionTwoDatasets(Dataset<Row> dataset1, Dataset<Row> dataset2) {
        return dataset1.unionByName(dataset2);
    }
    
    public static int ipv4ToInt(String ipv4) throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getByName(ipv4);
        byte[] bytes = inetAddress.getAddress();
        if (bytes.length != 4) {
            throw new IllegalArgumentException("Invalid IPv4 address.");
        }
        int result = 0;
        for (byte b : bytes) {
            result = (result << 8) | (b & 0xFF);
        }
        return result;
    }
    
    public static BigInteger ipv6ToInt(String ipv6) throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getByName(ipv6);
        byte[] bytes = inetAddress.getAddress();
        if (bytes.length != 16) {
            throw new IllegalArgumentException("Invalid IPv6 address.");
        }
        return new BigInteger(1, bytes);
    }
    
    public static Dataset<Row> performOptimizedRangeJoin(Dataset<Row> customerTable, Dataset<Row> timestampLookupTable) {
        // Convert customer dates to numeric for range filtering
        Dataset<Row> formattedCustomerTable = customerTable
                .withColumn("dt_numeric", functions.col("dt").cast("long"));

        Dataset<Row> formattedTimestampTable = timestampLookupTable
                .withColumn("start_date_numeric", functions.col("start_date").cast("long"))
                .withColumn("end_date_numeric", functions.col("end_date").cast("long"));

        // Broadcast the smaller dataset (timestamp lookup table)
        Dataset<Row> broadcastedTimestampTable = functions.broadcast(formattedTimestampTable);

        // Perform the range join using filter logic
        Dataset<Row> joinedData = formattedCustomerTable
                .join(broadcastedTimestampTable,
                        formattedCustomerTable.col("dt_numeric").geq(broadcastedTimestampTable.col("start_date_numeric"))
                                .and(formattedCustomerTable.col("dt_numeric").leq(broadcastedTimestampTable.col("end_date_numeric"))),
                        "left_outer")
                .select(
                        formattedCustomerTable.col("*"),
                        broadcastedTimestampTable.col("week_number")
                );

        return joinedData;
    }
    
    public static Dataset<Row> performOptimizedJoin(Dataset<Row> customerTable, Dataset<Row> ipTimestampIndex) {
        // Step 1: Join customerTable with ipTimestampIndex on IP_Address_One
        Dataset<Row> joinedData = customerTable.join(
                ipTimestampIndex,
                customerTable.col("IP_Address_One").equalTo(ipTimestampIndex.col("IP_Address_One")),
                "inner"
        );

        // Step 2: Filter and extract relevant elements from wk_ip_info
        Dataset<Row> filteredData = joinedData.withColumn(
                "matched_info",
                functions.expr(
                        "filter(wk_ip_info, info -> int(split(info, '~')[0]) = week_number)"
                )
        );

        // Step 3: Parse matched_info array directly without exploding
        Dataset<Row> parsedData = filteredData.withColumn(
                "parsed_info",
                functions.expr(
                        "transform(matched_info, info -> named_struct('week_number', split(info, '~')[0], 'pid', split(info, '~')[1], 'current_flag', split(info, '~')[2]))"
                )
        );

        // Step 4: Extract the first element of matched_info (since only one match is expected per week_number)
        Dataset<Row> finalData = parsedData.withColumn(
                "result_info",
                functions.expr("element_at(parsed_info, 1)") // Extract the first matching record
        );

        // Step 5: Add pid and current_flag as separate columns
        Dataset<Row> result = finalData
                .withColumn("pid", functions.col("result_info.pid"))
                .withColumn("current_flag", functions.col("result_info.current_flag"))
                .drop("matched_info", "parsed_info", "result_info");

        // Step 6: Select required columns for the final output
        return result.select(
                customerTable.col("Unique_Id"),
                customerTable.col("IP_Address_One"),
                customerTable.col("week_number"),
                result.col("pid"),
                result.col("current_flag")
        );
    }
}