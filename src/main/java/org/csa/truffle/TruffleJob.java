package org.csa.truffle;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.CloseableIterator;
import org.csa.truffle.function.ProcessFunctionJava;
import org.csa.truffle.function.ProcessFunctionPython;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Truffle Flink job.
 *
 * Reads three quarterly sales CSV files from the classpath, applies
 * {@link ProcessFunctionJava} (V1) and {@link ProcessFunctionPython} (V2)
 * to enrich each record, then writes the results to separate subdirectories
 * under {@code output/}.
 *
 * Run locally:
 *   mvn exec:java
 */
public class TruffleJob {

    private static final Logger log = LoggerFactory.getLogger(TruffleJob.class);

    static final String[] CSV_RESOURCES = {
            "data/sales_q1.csv",
            "data/sales_q2.csv",
            "data/sales_q3.csv"
    };

    public static void main(String[] args) throws Exception {
        log.info("Loading CSV resources: {}", Arrays.toString(CSV_RESOURCES));
        List<String> allLines = loadCsvLines();
        log.info("Loaded {} lines from {} CSV files", allLines.size(), CSV_RESOURCES.length);

        log.info("Running V1 (Java) transform");
        List<String> v1 = runTransform(allLines, new ProcessFunctionJava());
        log.info("V1 complete: {} output rows", v1.size());
        writeOutput(Paths.get("output", "v1", "sales_transformed.csv"), v1);

        log.info("Running V2 (Python) transform");
        List<String> v2 = runTransform(allLines, new ProcessFunctionPython());
        log.info("V2 complete: {} output rows", v2.size());
        writeOutput(Paths.get("output", "v2", "sales_transformed.csv"), v2);

        log.info("Done. Output written to output/v1/ and output/v2/");
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
