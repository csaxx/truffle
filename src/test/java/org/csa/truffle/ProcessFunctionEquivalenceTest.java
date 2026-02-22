package org.csa.truffle;

import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.csa.truffle.function.ProcessFunctionJava;
import org.csa.truffle.function.ProcessFunctionPython;
import org.junit.jupiter.api.Test;

import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProcessFunctionEquivalenceTest {

    private static final String[] CSV_RESOURCES = {"data/sales_q1.csv", "data/sales_q2.csv", "data/sales_q3.csv"};

    private List<String> loadAllLines() throws Exception {
        List<String> lines = new ArrayList<>();
        for (String r : CSV_RESOURCES) {
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(r)) {
                lines.addAll(IOUtils.readLines(is, StandardCharsets.UTF_8));
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

        List<String> out1 = new ArrayList<>(runThroughHarness(new ProcessFunctionJava(), input));
        List<String> out2 = new ArrayList<>(runThroughHarness(new ProcessFunctionPython(), input));

        assertFalse(out1.isEmpty(), "java produced no output");
        assertFalse(out2.isEmpty(), "python produced no output");

        Collections.sort(out1);
        Collections.sort(out2);
        assertEquals(out1, out2, "java and python must produce identical transformed output");
    }

    @Test
    void outputHasExpectedRecordCount() throws Exception {
        List<String> out = runThroughHarness(new ProcessFunctionJava(), loadAllLines());
        // 3 CSVs: 7 + 7 + 6 data rows (headers excluded by the ProcessFunction)
        assertEquals(20, out.size());
    }

    @Test
    void eachOutputLineHasEightFields() throws Exception {
        for (String line : runThroughHarness(new ProcessFunctionJava(), loadAllLines())) {
            assertEquals(8, line.split(",", -1).length,
                    "Expected 8 fields in: " + line);
        }
    }
}
