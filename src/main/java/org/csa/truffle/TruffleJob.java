package org.csa.truffle;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.CloseableIterator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Truffle Flink job.
 *
 * Reads three quarterly sales CSV files from the classpath, applies
 * {@link SalesTransformFunction} (V1) and {@link SalesTransformFunctionV2} (V2)
 * to enrich each record, then writes the results to separate subdirectories
 * under {@code output/}.
 *
 * Run locally:
 *   mvn exec:java
 */
public class TruffleJob {

    static final String[] CSV_RESOURCES = {
            "sales_q1.csv",
            "sales_q2.csv",
            "sales_q3.csv"
    };

    public static void main(String[] args) throws Exception {
        List<String> allLines = loadCsvLines();

        List<String> v1 = runTransform(allLines, new SalesTransformFunction());
        writeOutput(Paths.get("output", "v1", "sales_transformed.csv"), v1);

        List<String> v2 = runTransform(allLines, new SalesTransformFunctionV2());
        writeOutput(Paths.get("output", "v2", "sales_transformed.csv"), v2);

        System.out.println("Done. Output written to output/v1/ and output/v2/");
    }

    static List<String> loadCsvLines() throws Exception {
        List<String> lines = new ArrayList<>();
        for (String resource : CSV_RESOURCES) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    TruffleJob.class.getClassLoader().getResourceAsStream(resource)))) {
                lines.addAll(reader.lines().collect(Collectors.toList()));
            }
        }
        return lines;
    }

    static List<String> runTransform(List<String> input, ProcessFunction<String, String> fn)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<String> result = new ArrayList<>();
        try (CloseableIterator<String> it = env.fromData(input).process(fn).executeAndCollect()) {
            it.forEachRemaining(result::add);
        }
        return result;
    }

    private static void writeOutput(Path file, List<String> lines) throws Exception {
        Files.createDirectories(file.getParent());
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(file))) {
            w.println("transactionId,customerId,product,quantity,unitPrice,totalPrice,category,date");
            lines.forEach(w::println);
        }
    }
}
