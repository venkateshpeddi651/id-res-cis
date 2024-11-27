package com.identity.processing.udf;

import org.apache.spark.sql.api.java.UDF12;

import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("serial")
public class NameComparisonUDF implements UDF12<
        String, String, String, String, String, String, String, // Customer Input
        String, String, String, String, String,                // TU Input
        String[]> {

    @Override
    public String[] call(
            String custIpFirstName, String custIpPrefFirstName, String custIpFirstInitial,
            String custIpMiddleName, String custIpMiddleInitial, String custIpLastName, String custIpGenSuffix,
            String tuFirstName, String tuPrefFirstName, String tuLastName, String tuMiddleName, String tuGenSuffix) {

        // Derive Soundex Codes within the UDF
        var custIpSndxFirstName = deriveSoundex(custIpFirstName);
        var custIpSndxLastName = deriveSoundex(custIpLastName);
        var tuSndxFirstName = deriveSoundex(tuFirstName);
        var tuSndxLastName = deriveSoundex(tuLastName);

        // Initialize flags for match indicators
        var fnMatch = compare(custIpFirstName, tuFirstName);
        var prefFnMatch = "N".equals(fnMatch) ? comparePreferredNames(custIpPrefFirstName, tuPrefFirstName) : "I";
        var sndxFnMatch = "N".equals(fnMatch) && "N".equals(prefFnMatch) ? compare(custIpSndxFirstName, tuSndxFirstName) : "I";
        var finitMatch = "N".equals(fnMatch) && "N".equals(prefFnMatch) && "N".equals(sndxFnMatch)
                ? compareInitials(custIpFirstInitial, tuFirstName) : "I";

        var mnMatch = compare(custIpMiddleName, tuMiddleName);
        var minitMatch = "N".equals(mnMatch) ? compareInitials(custIpMiddleInitial, tuMiddleName) : "I";

        var lnMatch = compare(custIpLastName, tuLastName);
        var sndxLnMatch = "N".equals(lnMatch) ? compare(custIpSndxLastName, tuSndxLastName) : "I";
        var gensfxMatch = compare(custIpGenSuffix, tuGenSuffix);

        var fnRevslMatch = "N".equals(fnMatch) && "N".equals(prefFnMatch) && "N".equals(sndxFnMatch)
                && "N".equals(finitMatch) && "N".equals(lnMatch) && "N".equals(sndxLnMatch)
                ? compare(custIpLastName, tuFirstName) : "I";
        var lnRevslMatch = "N".equals(fnMatch) && "N".equals(prefFnMatch) && "N".equals(sndxFnMatch)
                && "N".equals(finitMatch) && "N".equals(lnMatch) && "N".equals(sndxLnMatch)
                ? compare(custIpFirstName, tuLastName) : "I";

        var prefFnRevslMatch = "N".equals(fnRevslMatch)
                ? comparePreferredNames(custIpPrefFirstName, tuPrefFirstName) : "I";
        var finitRevslMatch = "N".equals(fnRevslMatch) && "N".equals(prefFnRevslMatch)
                ? compareInitials(custIpLastName, tuFirstName) : "I";
        var sndxFnRevslMatch = "N".equals(fnRevslMatch) && "N".equals(prefFnRevslMatch)
                ? compare(custIpSndxLastName, tuSndxFirstName) : "I";
        var sndxLnRevslMatch = "N".equals(lnRevslMatch)
                ? compare(custIpSndxFirstName, tuSndxLastName) : "I";

        // Generate MATCH_KEY
        var matchKey = Stream.of(
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

    // Soundex Derivation
    private String deriveSoundex(String name) {
        if (name == null || name.isBlank()) {
            return "";
        }
        var soundex = new StringBuilder().append(Character.toUpperCase(name.charAt(0)));
        var mapping = "01230120022455012623010202".toCharArray();

        for (var i = 1; i < name.length(); i++) {
            var c = Character.toUpperCase(name.charAt(i));
            if (c >= 'A' && c <= 'Z') {
                var mapped = mapping[c - 'A'];
                if (mapped != '0' && soundex.charAt(soundex.length() - 1) != mapped) {
                    soundex.append(mapped);
                }
            }
        }

        while (soundex.length() < 4) {
            soundex.append('0');
        }

        return soundex.substring(0, 4);
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
