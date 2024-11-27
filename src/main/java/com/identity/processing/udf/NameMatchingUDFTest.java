package com.identity.processing.udf;

import java.util.Arrays;

public class NameMatchingUDFTest {

    public static void main(String[] args) {
        // Instantiate the NameMatchingUDF
        NameMatchingUDF nameMatchingUDF = new NameMatchingUDF();

        // Sample inputs
        String clientFirstName = "John";
        String clientLastName = "Doe";
        String clientMiddleName = "";
        String clientMiddleInitial = "A";
        String tuFirstName = "Jon";
        String tuLastName = "Doe";
        String tuMiddleName = "Allen";
        String tuMiddleInitial = "A";

        // Call the UDF's logic directly
        String[] result = nameMatchingUDF.call(
                clientFirstName,
                clientLastName,
                clientMiddleName,
                clientMiddleInitial,
                tuFirstName,
                tuLastName,
                tuMiddleName,
                tuMiddleInitial
        );

        // Print the results
        System.out.println("Rank: " + result[0]);
        System.out.println("MatchKey: " + result[1]);
    }
}
