package com.identity.processing.constants;

import java.util.HashMap;
import java.util.Map;

/**
 * Constants for MatchType and MatchLevel weights.
 */
public class MatchConstants {

    // Match type weights
    public static final Map<String, Integer> MATCH_TYPE_WEIGHTS = new HashMap<>();
    // Match level weights
    public static final Map<String, Integer> MATCH_LEVEL_WEIGHTS = new HashMap<>();

    static {
        // MatchType Weights
        MATCH_TYPE_WEIGHTS.put("NameAddress", 12);
        MATCH_TYPE_WEIGHTS.put("NameDOB", 11);
        MATCH_TYPE_WEIGHTS.put("NamePhone", 6);
        MATCH_TYPE_WEIGHTS.put("NameEmail", 10);
        MATCH_TYPE_WEIGHTS.put("NameMAID", 13);
        MATCH_TYPE_WEIGHTS.put("NameIP", 5);
        MATCH_TYPE_WEIGHTS.put("NameZIP11", 7);
        MATCH_TYPE_WEIGHTS.put("PhoneOnly", 4);
        MATCH_TYPE_WEIGHTS.put("EmailOnly", 8);
        MATCH_TYPE_WEIGHTS.put("MAIDOnly", 9);
        MATCH_TYPE_WEIGHTS.put("IPOnly", 1);
        MATCH_TYPE_WEIGHTS.put("AddressOnly", 2);
        MATCH_TYPE_WEIGHTS.put("ZIP11Only", 3);
        MATCH_TYPE_WEIGHTS.put("EXTRNLIDOnly", 99);

        // MatchLevel Weights
        MATCH_LEVEL_WEIGHTS.put("Individual", 2);
        MATCH_LEVEL_WEIGHTS.put("Household", 1);
    }
}
