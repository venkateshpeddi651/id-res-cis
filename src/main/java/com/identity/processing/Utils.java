package com.identity.processing;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

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
}