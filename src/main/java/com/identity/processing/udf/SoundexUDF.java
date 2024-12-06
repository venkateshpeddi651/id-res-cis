package com.identity.processing.udf;

import org.apache.spark.sql.api.java.UDF1;

import java.util.HashMap;
import java.util.Map;

public class SoundexUDF implements UDF1<String, String> {

    // Full five-bit conversion map
    private static final Map<Character, Integer> fiveBitConversion = new HashMap<>();
    private static final Map<Character, Integer> fourBitConversion = new HashMap<>();

    static {
        // Populate five-bit conversion for A-Z
        for (char ch = 'A'; ch <= 'Z'; ch++) {
            fiveBitConversion.put(ch, ch - 'A' + 1); // 'A' -> 1, 'B' -> 2, ..., 'Z' -> 26
        }

        // Populate four-bit conversion for A-Z
        fourBitConversion.put('A', 1);
        fourBitConversion.put('B', 2);
        fourBitConversion.put('C', 3);
        fourBitConversion.put('D', 4);
        fourBitConversion.put('E', 1);
        fourBitConversion.put('F', 2);
        fourBitConversion.put('G', 5);
        fourBitConversion.put('H', 6);
        fourBitConversion.put('I', 1);
        fourBitConversion.put('J', 5);
        fourBitConversion.put('K', 3);
        fourBitConversion.put('L', 7);
        fourBitConversion.put('M', 8);
        fourBitConversion.put('N', 8);
        fourBitConversion.put('O', 1);
        fourBitConversion.put('P', 2);
        fourBitConversion.put('Q', 9);
        fourBitConversion.put('R', 10);
        fourBitConversion.put('S', 3);
        fourBitConversion.put('T', 4);
        fourBitConversion.put('U', 1);
        fourBitConversion.put('V', 2);
        fourBitConversion.put('W', 11);
        fourBitConversion.put('X', 12);
        fourBitConversion.put('Y', 1);
        fourBitConversion.put('Z', 3);
    }

    @Override
    public String call(String input) {
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

        // Step 3: Handle single-character input
        if (inputLength == 1) {
            Integer firstByte = fiveBitConversion.getOrDefault(inputChars[0], 0);
            return String.format("%08X", firstByte << 25); // Pad to 25 bits
        }

        // Step 4: Conversion logic for longer names
        int g2Soundex = 0;
        Integer firstByte = fiveBitConversion.getOrDefault(inputChars[0], 0);
        Integer secondByte = fiveBitConversion.getOrDefault(inputChars[1], 0);
        Integer prevValue = fourBitConversion.getOrDefault(inputChars[1], 0);

        g2Soundex = (firstByte << 5) | secondByte; // Add first two 5-bit codes

        int letterCount = 2;
        int tempValue = 0;

        // Process remaining characters
        for (int i = 2; i < inputLength; i++) {
            tempValue = fourBitConversion.getOrDefault(inputChars[i], 0);

            // Skip duplicates
            if (tempValue != prevValue) {
                g2Soundex = (g2Soundex << 4) | tempValue;
                letterCount++;
                prevValue = tempValue;
            }

            // Handle truncation logic for values 1 and 3
            if (letterCount > 3 && (tempValue == 1 || tempValue == 3)) {
                g2Soundex >>= 4; // Remove last code
                letterCount--;
            }
        }

        // Pad to ensure at least 6 characters
        for (int i = letterCount; i < 6; i++) {
            g2Soundex <<= 4; // Add padding
        }

        // Append extra characters count
        if (letterCount > 6) {
            int extraChars = Math.min(9, letterCount - 6);
            g2Soundex = (g2Soundex << 4) | extraChars;
        }

        // Convert to little-endian hex representation
        return String.format("%08X", g2Soundex);
    }
}
