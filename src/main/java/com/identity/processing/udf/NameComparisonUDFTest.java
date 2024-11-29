package com.identity.processing.udf;

import com.identity.processing.udf.NameComparisonUDF;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NameComparisonUDFTest {

    private static NameComparisonUDF nameComparisonUDF;

    @BeforeAll
    static void setUp() {
        nameComparisonUDF = new NameComparisonUDF();
    }

    @Test
    void testYIIIYIYIIIIIII() {
        String[] result = nameComparisonUDF.call(
                "John", "Johnny", "J",
                "Allen", "A", "Doe", null,
                "John", "Johnny", "Doe", "Allen", null,
                "J500", "D000", "J500", "D000"
        );
        assertEquals("YIIIYIYIIIIIII", result[15]);
    }

    @Test
    void testNYIIYIYIIIIIII() {
        String[] result = nameComparisonUDF.call(
                "Jon", "Johnny", "J",
                "Allen", "A", "Doe", null,
                "John", "Johnny", "Doe", "Allen", null,
                "J500", "D000", "J500", "D000"
        );
        assertEquals("NYIIYIYIIIIIII", result[15]);
    }

    @Test
    void testNNIYYIYIIIIIII() {
        String[] result = nameComparisonUDF.call(
                "Jon", "Johnny", "J",
                "Brian", "B", "Doe", null,
                "John", "Johnny", "Doe", "Brian", null,
                "J500", "D000", "J500", "D000"
        );
        assertEquals("NNIYYIYIIIIIII", result[15]);
    }

    @Test
    void testNNYNYIYIIIIIII() {
        String[] result = nameComparisonUDF.call(
                "Jon", "Jack", "J",
                "Brian", "B", "Doe", null,
                "John", "Johnny", "Smith", "Brian", null,
                "J500", "D000", "J500", "S530"
        );
        assertEquals("NNYNYIYIIIIIII", result[15]);
    }

    @Test
    void testYIIINYYIIIIIII() {
        String[] result = nameComparisonUDF.call(
                "John", "Johnny", "J",
                "Allen", "A", "Doe", "Jr.",
                "John", "Johnny", "Doe", "Brian", null,
                "J500", "D000", "J500", "D000"
        );
        assertEquals("YIIINYYIIIIIII", result[15]);
    }

    @Test
    void testNYIINYYIIIIIII() {
        String[] result = nameComparisonUDF.call(
                "Jon", "Johnny", "J",
                "Brian", "B", "Doe", "Sr.",
                "John", "Johnny", "Doe", "Brian", null,
                "J500", "D000", "J500", "D000"
        );
        assertEquals("NYIINYYIIIIIII", result[15]);
    }

    @Test
    void testNNIYNYYIIIIIII() {
        String[] result = nameComparisonUDF.call(
                "Jon", "Johnny", "J",
                "Brian", "B", "Smith", null,
                "John", "Johnny", "Doe", "Brian", null,
                "J500", "S530", "J500", "D000"
        );
        assertEquals("NNIYNYYIIIIIII", result[15]);
    }

    @Test
    void testNNYNNYYIIIIIII() {
        String[] result = nameComparisonUDF.call(
                "Jane", "Janet", "J",
                "Marie", "M", "Smith", null,
                "John", "Johnny", "Doe", "Allen", null,
                "J500", "S530", "J500", "D000"
        );
        assertEquals("NNYNNYYIIIIIII", result[15]);
    }

    @Test
    void testYIIIYINYIIIIII() {
        String[] result = nameComparisonUDF.call(
                "John", "Johnny", "J",
                "Allen", "A", "Doe", "Jr.",
                "John", "Johnny", "Doe", "Allen", null,
                "J500", "D000", "J500", "D000"
        );
        assertEquals("YIIIYINYIIIIII", result[15]);
    }

    @Test
    void testNYIIYINYIIIIII() {
        String[] result = nameComparisonUDF.call(
                "Jon", "Johnny", "J",
                "Allen", "A", "Doe", "Jr.",
                "John", "Johnny", "Doe", "Allen", null,
                "J500", "D000", "J500", "D000"
        );
        assertEquals("NYIIYINYIIIIII", result[15]);
    }

    @Test
    void testNNNNNNNNNNNNNNN() {
        String[] result = nameComparisonUDF.call(
                "Unknown", "N/A", null,
                null, null, "Unknown", null,
                null, null, "Unknown", null, null,
                "U500", "U000", "U500", "U000"
        );
        assertEquals("NNNNNNNNNNNNNNN", result[15]);
    }

    @Test
    void testYIIIIINNIIIIII() {
        String[] result = nameComparisonUDF.call(
                "John", null, "J",
                null, null, "Smith", null,
                "TU_John", null, "TU_Smith", null, null,
                "J500", "S530", "J500", "S530"
        );
        assertEquals("YIIIIINNIIIIII", result[15]);
    }

    // Add all remaining cases in a similar manner...
    // Each test will match a specific MATCHKEY value.
}
