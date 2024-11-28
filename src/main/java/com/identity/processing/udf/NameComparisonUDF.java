package com.identity.processing.udf;

import org.apache.spark.sql.api.java.UDF16;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NameComparisonUDF implements UDF16<
        String, String, String, String, String, String, String, // Customer Input
        String, String, String, String, String, String, String, // TU Input
        String, String,                                          // Soundex Input
        String[]> {

    @Override
    public String[] call(
            String custIpFirstName, String custIpPrefFirstName, String custIpFirstInitial,
            String custIpMiddleName, String custIpMiddleInitial, String custIpLastName, String custIpGenSuffix,
            String tuFirstName, String tuPrefFirstName, String tuLastName, String tuMiddleName, String tuGenSuffix,
            String custIpSndxFirstName, String custIpSndxLastName, String tuSndxFirstName, String tuSndxLastName) {

        // Initialize flags for match indicators
        String fnMatch = compare(custIpFirstName, tuFirstName);
        String prefFnMatch = "N".equals(fnMatch) ? comparePreferredNames(custIpPrefFirstName, tuPrefFirstName) : "I";
        String sndxFnMatch = "N".equals(fnMatch) && "N".equals(prefFnMatch) ? compare(custIpSndxFirstName, tuSndxFirstName) : "I";
        String finitMatch = "N".equals(fnMatch) && "N".equals(prefFnMatch) && "N".equals(sndxFnMatch)
                ? compareInitials(custIpFirstInitial, tuFirstName) : "I";

        String mnMatch = compare(custIpMiddleName, tuMiddleName);
        String minitMatch = "N".equals(mnMatch) ? compareInitials(custIpMiddleInitial, tuMiddleName) : "I";

        String lnMatch = compare(custIpLastName, tuLastName);
        String sndxLnMatch = "N".equals(lnMatch) ? compare(custIpSndxLastName, tuSndxLastName) : "I";
        String gensfxMatch = compare(custIpGenSuffix, tuGenSuffix);

        String fnRevslMatch = "N".equals(fnMatch) && "N".equals(prefFnMatch) && "N".equals(sndxFnMatch)
                && "N".equals(finitMatch) && "N".equals(lnMatch) && "N".equals(sndxLnMatch)
                ? compare(custIpLastName, tuFirstName) : "I";
        String lnRevslMatch = "N".equals(fnMatch) && "N".equals(prefFnMatch) && "N".equals(sndxFnMatch)
                && "N".equals(finitMatch) && "N".equals(lnMatch) && "N".equals(sndxLnMatch)
                ? compare(custIpFirstName, tuLastName) : "I";

        String prefFnRevslMatch = "N".equals(fnRevslMatch)
                ? comparePreferredNames(custIpPrefFirstName, tuPrefFirstName) : "I";
        String finitRevslMatch = "N".equals(fnRevslMatch) && "N".equals(prefFnRevslMatch)
                ? compareInitials(custIpLastName, tuFirstName) : "I";
        String sndxFnRevslMatch = "N".equals(fnRevslMatch) && "N".equals(prefFnRevslMatch)
                ? compare(custIpSndxLastName, tuSndxFirstName) : "I";
        String sndxLnRevslMatch = "N".equals(lnRevslMatch)
                ? compare(custIpSndxFirstName, tuSndxLastName) : "I";

        // Generate MATCH_KEY
        String matchKey = Stream.of(
                fnMatch, prefFnMatch, finitMatch, sndxFnMatch, mnMatch, minitMatch,
                lnMatch, sndxLnMatch, fnRevslMatch, lnRevslMatch, prefFnRevslMatch,
                finitRevslMatch, sndxFnRevslMatch, sndxLnRevslMatch
        ).collect(Collectors.joining(""));

        // Return match indicators and match key
        return new String[]{
                fnMatch, prefFnMatch, finitMatch, sndxFnMatch, mnMatch, minitMatch,
                lnMatch, sndxLnMatch, gensfxMatch, fnRevslMatch, lnRevslMatch,
                prefFnRevslMatch, finitRevslMatch, sndxFnRevslMatch, sndxLnRevslMatch,
                matchKey
        };
    }

    // Comparison utilities
    private String compare(String value1, String value2) {
        if (isBlank(value1) || isBlank(value2)) {
            return "I";
        }
        return value1.equalsIgnoreCase(value2) ? "Y" : "N";
    }

    private String comparePreferredNames(String value1, String value2) {
        if ("N/A".equalsIgnoreCase(value1) || "N/A".equalsIgnoreCase(value2)) {
            return "N";
        }
        return compare(value1, value2);
    }

    private String compareInitials(String value1, String value2) {
        if (isBlank(value1) || isBlank(value2)) {
            return "I";
        }
        return value1.substring(0, 1).equalsIgnoreCase(value2.substring(0, 1)) ? "Y" : "N";
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
