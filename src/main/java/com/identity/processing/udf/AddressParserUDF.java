package com.identity.processing.udf;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import java.util.*;
import java.util.regex.Pattern;

public class AddressParserUDF implements UDF1<Row, Row> {

    private static final Map<String, String> DIRECTIONALS = createDirectionalMap();
    private static final Map<String, String> STREET_SUFFIXES = createStreetSuffixMap();
    private static final Map<String, String> PO_BOX_TERMS = createPOBoxMap();
    private static final Map<String, String> RURAL_ROUTE_TERMS = createRuralRouteMap();
    private static final Map<String, String> ORDINALS = createOrdinalsLookup();
    private static final Map<String, String> NUMERALS = createNumeralsLookup();
    private static final Pattern EXTRA_SPACES = Pattern.compile("\\s+");
    private static final Pattern NUMERIC_PATTERN = Pattern.compile("^\\d+$");
    private static final Pattern ALPHA_NUMERIC_PATTERN = Pattern.compile("^[A-Z]*\\d+[A-Z]*$");

    @Override
    public Row call(Row input) {
        // Input fields
        String addrLine1 = input.getAs("Street_Address_Line_1");
        String addrLine2 = input.getAs("Street_Address_Line_2");
        String city = input.getAs("City");
        String state = input.getAs("State");
        String zipCode = input.getAs("ZIP_Code");

        // Output fields initialization
        String addrNum = "";
        String predirCde = "";
        String strName = "";
        String prmStrTypCde = "";
        String postdirCde = "";
        String untNum = "";
        String untTypNme = "";
        String zipExt = "";
        String warnings = "";
        String errors = "";

        try {
            // Step 1: Preprocess Address Line 1
            if (addrLine1 != null) {
                String normalizedAddr = preprocessAddress(addrLine1);
                normalizedAddr = applyClean1(normalizedAddr);
                normalizedAddr = applyClean2(normalizedAddr);
                normalizedAddr = applyClean3(normalizedAddr);

                String[] words = normalizedAddr.split(" ");

                if (isPOBox(words)) {
                    strName = "PO BOX";
                    addrNum = extractAfter(normalizedAddr, "PO BOX");
                } else if (isRuralRoute(words)) {
                    strName = "RURAL ROUTE";
                    addrNum = extractAfter(normalizedAddr, "RR");
                } else {
                    // Address number extraction
                    addrNum = extractAddressNumber(words);
                    
                    // Handle concatenated pre-directional codes with numbers
                    String potentialPredir = extractPreDirectional(words);
                    if (!potentialPredir.isEmpty() && addrNum.matches(".*[A-Z].*")) {
                        predirCde = potentialPredir;
                        addrNum = addrNum.replace(predirCde, "");
                    } else {
                        predirCde = potentialPredir;
                    }

                    // Extract street name and type
                    strName = extractStreetName(words, addrNum, predirCde);
                    prmStrTypCde = extractStreetSuffix(words);

                    // Post-directional codes
                    postdirCde = extractPostDirectional(words);

                    // Adjust cases where street name is mistakenly identified as directional
                    if (strName.isEmpty() && !predirCde.isEmpty() && STREET_SUFFIXES.containsKey(predirCde)) {
                        strName = predirCde;
                        predirCde = "";
                    }

                    // Adjust cases where suffix is appended incorrectly to the street name
                    if (strName.isEmpty() && !prmStrTypCde.isEmpty() && STREET_SUFFIXES.containsKey(prmStrTypCde)) {
                        strName = prmStrTypCde;
                        prmStrTypCde = "";
                    }
                }
            }

            // Step 2: Handle Address Line 2 for Unit Info
            if (addrLine2 != null && !addrLine2.isEmpty()) {
                String[] addrLine2Words = preprocessAddress(addrLine2).split(" ");
                if (addrLine2Words.length > 1) {
                    untTypNme = addrLine2Words[0];
                    untNum = applyNumeralsLookup(addrLine2Words[1]);
                }
            }

            // Step 3: Process ZIP Code for Extension
            zipExt = extractZipExtension(zipCode);

            // Step 4: Validate and Generate Warnings
            if (addrNum.isEmpty() || strName.isEmpty()) {
                warnings = "Incomplete address information.";
            }

        } catch (Exception e) {
            errors = "Error parsing address: " + e.getMessage();
        }

        // Return structured output
        return RowFactory.create(addrNum, predirCde, strName, prmStrTypCde, postdirCde, untTypNme, untNum, city, state, zipCode, zipExt, warnings, errors);
    }

    private String preprocessAddress(String address) {
        if (address == null || address.isEmpty()) return "";
        address = address.toUpperCase().trim();
        address = EXTRA_SPACES.matcher(address).replaceAll(" ");
        address = address.replaceAll("\\.", "");
        return address;
    }

    private String applyClean1(String input) {
        return input.replaceAll("[\\.\\-#]", " ").replaceAll("  +", " ").trim();
    }

    private String applyClean2(String input) {
        return input.replaceAll("^P O BOX |^PO BOX |^BOX ", "POB ");
    }

    private String applyClean3(String input) {
        return input.replaceAll("^RURAL ROUTE |^RR ", "RR ");
    }

    private boolean isPOBox(String[] words) {
        return words.length > 0 && PO_BOX_TERMS.containsKey(words[0]);
    }

    private boolean isRuralRoute(String[] words) {
        return words.length > 0 && RURAL_ROUTE_TERMS.containsKey(words[0]);
    }

    private String extractAddressNumber(String[] words) {
        for (String word : words) {
            if (NUMERIC_PATTERN.matcher(word).matches() || ALPHA_NUMERIC_PATTERN.matcher(word).matches()) {
                return applyOrdinalsLookup(word);
            }
        }
        return "";
    }

    private String applyOrdinalsLookup(String word) {
        return ORDINALS.getOrDefault(word, word);
    }

    private String applyNumeralsLookup(String word) {
        return NUMERALS.getOrDefault(word, word);
    }

    private String extractPreDirectional(String[] words) {
        if (words.length > 0 && DIRECTIONALS.containsKey(words[0])) {
            return DIRECTIONALS.get(words[0]);
        }
        return "";
    }

    private String extractStreetName(String[] words, String addrNum, String predirCde) {
        StringBuilder nameBuilder = new StringBuilder();
        boolean startAdding = false;
        for (String word : words) {
            // Skip the address number and predirectional code
            if (word.equals(addrNum) || word.equals(predirCde)) {
                startAdding = true;
                continue;
            }
            // Add valid street name parts
            if (startAdding && !NUMERIC_PATTERN.matcher(word).matches() && !STREET_SUFFIXES.containsKey(word) && !DIRECTIONALS.containsKey(word)) {
                nameBuilder.append(word).append(" ");
            }
        }
        return nameBuilder.toString().trim();
    }

    private String extractStreetSuffix(String[] words) {
        for (String word : words) {
            if (STREET_SUFFIXES.containsKey(word)) {
                return STREET_SUFFIXES.get(word);
            }
        }
        return "";
    }

    private String extractPostDirectional(String[] words) {
        if (words.length > 1 && DIRECTIONALS.containsKey(words[words.length - 1])) {
            return DIRECTIONALS.get(words[words.length - 1]);
        }
        return "";
    }

    private String extractZipExtension(String zip) {
        if (zip != null && zip.length() > 5) {
            return zip.substring(5).trim();
        }
        return "";
    }

    private static Map<String, String> createDirectionalMap() {
        Map<String, String> map = new HashMap<>();
        map.put("NORTH", "N"); map.put("N", "N");
        map.put("SOUTH", "S"); map.put("S", "S");
        map.put("EAST", "E"); map.put("E", "E");
        map.put("WEST", "W"); map.put("W", "W");
        map.put("NORTHEAST", "NE"); map.put("NE", "NE");
        map.put("NORTHWEST", "NW"); map.put("NW", "NW");
        map.put("SOUTHEAST", "SE"); map.put("SE", "SE");
        map.put("SOUTHWEST", "SW"); map.put("SW", "SW");
        return map;
    }

    private static Map<String, String> createStreetSuffixMap() {
        Map<String, String> map = new HashMap<>();
        map.put("STREET", "ST"); map.put("STR", "ST");
        map.put("AVENUE", "AVE"); map.put("AVE", "AVE");
        map.put("ROAD", "RD"); map.put("RD", "RD");
        map.put("BOULEVARD", "BLVD"); map.put("BLVD", "BLVD");
        map.put("DRIVE", "DR"); map.put("DR", "DR");
        map.put("PLACE", "PL"); map.put("PL", "PL");
        map.put("COURT", "CT"); map.put("CT", "CT");
        map.put("LANE", "LN"); map.put("LN", "LN");
        map.put("CIRCLE", "CIR"); map.put("CIR", "CIR");
        map.put("PARKWAY", "PKWY"); map.put("PKWY", "PKWY");
        map.put("WAY", "WAY");
        return map;
    }

    private static Map<String, String> createPOBoxMap() {
        Map<String, String> map = new HashMap<>();
        map.put("P O BOX", "PO BOX"); map.put("POB", "PO BOX");
        map.put("BOX", "PO BOX");
        return map;
    }

    private static Map<String, String> createRuralRouteMap() {
        Map<String, String> map = new HashMap<>();
        map.put("RURAL ROUTE", "RURAL ROUTE"); map.put("R R", "RURAL ROUTE");
        map.put("RR", "RURAL ROUTE");
        return map;
    }

    private static Map<String, String> createOrdinalsLookup() {
        Map<String, String> map = new HashMap<>();
        map.put("FIRST", "1ST"); map.put("1ST", "1ST");
        map.put("SECOND", "2ND"); map.put("2ND", "2ND");
        map.put("THIRD", "3RD"); map.put("3RD", "3RD");
        map.put("FOURTH", "4TH"); map.put("4TH", "4TH");
        map.put("FIFTH", "5TH"); map.put("5TH", "5TH");
        return map;
    }

    private static Map<String, String> createNumeralsLookup() {
        Map<String, String> map = new HashMap<>();
        map.put("FIRST", "1"); map.put("1ST", "1"); map.put("1", "1");
        map.put("SECOND", "2"); map.put("2ND", "2"); map.put("2", "2");
        map.put("THIRD", "3"); map.put("3RD", "3"); map.put("3", "3");
        map.put("FOURTH", "4"); map.put("4TH", "4"); map.put("4", "4");
        map.put("FIFTH", "5"); map.put("5TH", "5"); map.put("5", "5");
        return map;
    }
    
    private String extractAfter(String input, String marker) {
        if (input.contains(marker)) {
            return input.substring(input.indexOf(marker) + marker.length()).trim();
        }
        return "";
    }
}

