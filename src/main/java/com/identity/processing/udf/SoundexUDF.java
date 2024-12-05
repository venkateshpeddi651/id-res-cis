package com.identity.processing.udf;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SoundexUDF {

    // Predefined maps for five-bit and four-bit conversions
    private static final Map<Character, Integer> fiveBitConversion = new HashMap<>();
    private static final Map<Character, Integer> fourBitConversion = new HashMap<>();

    static {
        // Populate conversion maps based on the Ab Initio logic
        // Example initialization (fill in with actual mapping logic from your business rules)
        fiveBitConversion.put('A', 1); // Example
        fiveBitConversion.put('B', 2); // Example
        // Populate all other characters

        fourBitConversion.put('A', 1); // Example
        fourBitConversion.put('B', 2); // Example
        // Populate all other characters
    }

    public static UDF1<String, String> soundexUDF() {
        return (String input) -> {
            if (input == null || input.isEmpty()) {
                return null;
            }

            // Step 1: Filter out invalid characters
            String filteredInput = input.replaceAll("[^a-zA-Z]", "").toUpperCase();
            if (filteredInput.isEmpty()) {
                return null;
            }

            // Step 2: Convert to character array
            char[] inputChars = filteredInput.toCharArray();
            int inputLength = inputChars.length;

            // Step 3: Handle short names
            if (inputLength == 1) {
                Integer firstByte = fiveBitConversion.getOrDefault(inputChars[0], 0);
                return String.format("%08x", firstByte << 25); // Pad to 25 bits
            }

            // Step 4: Conversion logic for longer names
            int g2Soundex = 0;
            Integer firstByte = fiveBitConversion.getOrDefault(inputChars[0], 0);
            Integer secondByte = fiveBitConversion.getOrDefault(inputChars[1], 0);
            Integer prevValue = fourBitConversion.getOrDefault(inputChars[1], 0);

            g2Soundex = (firstByte << 5) | secondByte; // Add first two 5-bit codes

            int letterCount = 2;
            int tempValue = 0;

            for (int i = 2; i < inputLength; i++) {
                tempValue = fourBitConversion.getOrDefault(inputChars[i], 0);

                if (tempValue != prevValue) { // Skip duplicates
                    g2Soundex = (g2Soundex << 4) | tempValue;
                    letterCount++;
                    prevValue = tempValue;
                }

                // Handle truncation logic for 1 and 3
                if (letterCount > 3 && (tempValue == 1 || tempValue == 3)) {
                    g2Soundex >>= 4; // Remove last code
                    letterCount--;
                }
            }

            // Padding to ensure at least 6 characters
            for (int i = letterCount; i < 6; i++) {
                g2Soundex <<= 4; // Add padding
            }

            // Append extra characters count
            if (letterCount > 6) {
                int extraChars = Math.min(9, letterCount - 6);
                g2Soundex = (g2Soundex << 4) | extraChars;
            }

            // Convert to little-endian hex representation
            return String.format("%08x", g2Soundex);
        };
    }
}
