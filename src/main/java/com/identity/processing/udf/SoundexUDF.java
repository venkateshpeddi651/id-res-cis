package com.identity.processing.udf;

import org.apache.spark.sql.api.java.UDF1;

import java.util.HashMap;
import java.util.Map;

public class SoundexUDF implements UDF1<String, String> {

    private static final Map<Character, Integer> fiveBitConversion = new HashMap<>();
    private static final Map<Character, Integer> fourBitConversion = new HashMap<>();

    static {
        // Initialize the five-bit conversion table
        fiveBitConversion.put('A', 1);
        fiveBitConversion.put('B', 2);
        // Populate all other characters for five-bit conversion
        fiveBitConversion.put('M', 12); // For `M` as per Ab Initio logic

        // Initialize the four-bit conversion table
        fourBitConversion.put('A', 1);
        fourBitConversion.put('B', 2);
        fourBitConversion.put('R', 10); // Example: Corresponds to `A` in hex
        fourBitConversion.put('L', 7);
        fourBitConversion.put('Y', 1);
        fourBitConversion.put('N', 8);
        fourBitConversion.put('D', 4);
        // Populate the rest of the alphabet
    }

    @Override
    public String call(String input) throws Exception {
        if (input == null || input.isEmpty()) {
            return null;
        }

        // Filter and uppercase
        String filteredInput = input.replaceAll("[^a-zA-Z]", "").toUpperCase();
        if (filteredInput.isEmpty()) {
            return null;
        }

        int soundex = 0;
        int letterCount = 0;

        // First two letters -> Five-bit conversion
        if (filteredInput.length() > 1) {
            char firstChar = filteredInput.charAt(0);
            char secondChar = filteredInput.charAt(1);
            soundex |= fiveBitConversion.getOrDefault(firstChar, 0) << 5;
            soundex |= fiveBitConversion.getOrDefault(secondChar, 0);
            letterCount += 2;
        }

        // Remaining letters -> Four-bit conversion
        for (int i = 2; i < filteredInput.length(); i++) {
            char currentChar = filteredInput.charAt(i);
            int fourBitCode = fourBitConversion.getOrDefault(currentChar, 0);

            if (letterCount < 6) {
                soundex = (soundex << 4) | fourBitCode;
                letterCount++;
            }
        }

        // Padding to 8 characters
        while (letterCount < 6) {
            soundex = soundex << 4;
            letterCount++;
        }

        // Convert to 8-character hexadecimal
        return String.format("%08X", soundex);
    }
}
