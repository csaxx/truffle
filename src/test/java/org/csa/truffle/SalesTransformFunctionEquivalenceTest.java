package org.csa.truffle;

import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class SalesTransformFunctionEquivalenceTest {

    private static final String[] CSV_RESOURCES = {"sales_q1.csv", "sales_q2.csv", "sales_q3.csv"};

    private List<String> loadAllLines() throws Exception {
        List<String> lines = new ArrayList<>();
        for (String r : CSV_RESOURCES) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    getClass().getClassLoader().getResourceAsStream(r)))) {
                lines.addAll(reader.lines().collect(Collectors.toList()));
            }
        }
        return lines;
    }

    private List<String> runThroughHarness(
            org.apache.flink.streaming.api.functions.ProcessFunction<String, String> fn,
            List<String> inputLines) throws Exception {
        try (OneInputStreamOperatorTestHarness<String, String> h =
                ProcessFunctionTestHarnesses.forProcessFunction(fn)) {
            h.open();
            for (String line : inputLines) {
                h.processElement(line, 0L);
            }
            return h.extractOutputValues();
        }
    }

    @Test
    void v1AndV2ProduceIdenticalOutput() throws Exception {
        List<String> input = loadAllLines();

        List<String> out1 = new ArrayList<>(runThroughHarness(new SalesTransformFunction(), input));
        List<String> out2 = new ArrayList<>(runThroughHarness(new SalesTransformFunctionV2(), input));

        assertFalse(out1.isEmpty(), "V1 produced no output");
        assertFalse(out2.isEmpty(), "V2 produced no output");

        Collections.sort(out1);
        Collections.sort(out2);
        assertEquals(out1, out2, "V1 and V2 must produce identical transformed output");
    }

    @Test
    void outputHasExpectedRecordCount() throws Exception {
        List<String> out = runThroughHarness(new SalesTransformFunction(), loadAllLines());
        // 3 CSVs: 7 + 7 + 6 data rows (headers excluded by the ProcessFunction)
        assertEquals(20, out.size());
    }

    @Test
    void eachOutputLineHasEightFields() throws Exception {
        for (String line : runThroughHarness(new SalesTransformFunction(), loadAllLines())) {
            assertEquals(8, line.split(",", -1).length,
                    "Expected 8 fields in: " + line);
        }
    }
}
