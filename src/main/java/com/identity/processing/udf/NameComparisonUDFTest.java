package com.identity.processing.udf;

public class NameComparisonUDFTest {
    public static void main(String[] args) {
        // Instantiate the NameComparisonUDF
        NameComparisonUDF nameComparisonUDF = new NameComparisonUDF();

        // Sample inputs
        String custIpFirstName = "John";
        String custIpPrefFirstName = "Johnny";
        String custIpFirstInitial = "J";
        String custIpMiddleName = "Allen";
        String custIpMiddleInitial = "A";
        String custIpLastName = "Doe";
        String custIpGenSuffix = "Jr.";

        String tuFirstName = "Jon";
        String tuPrefFirstName = "Johnny";
        String tuLastName = "Doe";
        String tuMiddleName = "Allen";
        String tuGenSuffix = "Jr.";

        // Call the UDF
        String[] result = nameComparisonUDF.call(
                custIpFirstName, custIpPrefFirstName, custIpFirstInitial,
                custIpMiddleName, custIpMiddleInitial, custIpLastName, custIpGenSuffix,
                tuFirstName, tuPrefFirstName, tuLastName, tuMiddleName, tuGenSuffix
        );

        // Print the results
        System.out.println("Match Indicators:");
        System.out.println("FN_Match: " + result[0]);
        System.out.println("PFN_Match: " + result[1]);
        System.out.println("FINIT_Match: " + result[2]);
        System.out.println("SNDXFN_Match: " + result[3]);
        System.out.println("MN_Match: " + result[4]);
        System.out.println("MINIT_Match: " + result[5]);
        System.out.println("LN_Match: " + result[6]);
        System.out.println("SNDXLN_Match: " + result[7]);
        System.out.println("GENSFX_Match: " + result[8]);
        System.out.println("FNREVSL_Match: " + result[9]);
        System.out.println("LNREVSL_Match: " + result[10]);
        System.out.println("PFNREVSL_Match: " + result[11]);
        System.out.println("FINITREVSL_Match: " + result[12]);
        System.out.println("SNDXFNREVSL_Match: " + result[13]);
        System.out.println("SNDXLNREVSL_Match: " + result[14]);
        System.out.println("Match Key: " + result[15]);
    }
}
