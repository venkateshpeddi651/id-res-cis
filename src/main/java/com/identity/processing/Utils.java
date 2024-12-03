package com.identity.processing;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class Utils {

    public static double levenshteinDistance(String s1, String s2) {
        // Implement Levenshtein distance calculation here
        return 0.0;
    }
    
    public static Dataset<Row> unionDatasets(Dataset<Row> dataset1, Dataset<Row> dataset2) {
        // Get schema differences
        List<String> schema1Columns = Stream.of(dataset1.columns()).collect(Collectors.toList());
        List<String> schema2Columns = Stream.of(dataset2.columns()).collect(Collectors.toList());

        // Add missing columns to dataset1
        for (String column : schema2Columns) {
            if (!schema1Columns.contains(column)) {
                dataset1 = dataset1.withColumn(column, functions.lit(null));
            }
        }

        // Add missing columns to dataset2
        for (String column : schema1Columns) {
            if (!schema2Columns.contains(column)) {
                dataset2 = dataset2.withColumn(column, functions.lit(null));
            }
        }

        // Align column order
        Dataset<Row> alignedDataset1 = dataset1.select((Column[]) Stream.of(dataset2.columns()).map(functions::col).toArray());
        Dataset<Row> alignedDataset2 = dataset2.select((Column[]) Stream.of(dataset1.columns()).map(functions::col).toArray());

        // Perform union
        return alignedDataset1.unionByName(alignedDataset2);
    }
}