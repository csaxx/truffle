package org.csa.truffle;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
 * {@link SalesTransformFunction} to enrich each record, then writes the
 * results to {@code output/sales_transformed.csv} in the working directory.
 *
 * Run locally:
 *   mvn exec:java
 */
public class TruffleJob {

    private static final String[] CSV_RESOURCES = {
            "sales_q1.csv",
            "sales_q2.csv",
            "sales_q3.csv"
    };

    private static final Path OUTPUT_FILE = Paths.get("output", "sales_transformed.csv");

    public static void main(String[] args) throws Exception {

        // --- load CSV lines from classpath -----------------------------------
        List<String> allLines = new ArrayList<>();
        for (String resource : CSV_RESOURCES) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    TruffleJob.class.getClassLoader().getResourceAsStream(resource)))) {
                allLines.addAll(reader.lines().collect(Collectors.toList()));
            }
        }

        // --- build the Flink pipeline ----------------------------------------
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> rawStream = env.fromData(allLines);

        DataStream<String> transformed = rawStream.process(new SalesTransformFunction());

        // --- collect results and write to disk --------------------------------
        Files.createDirectories(OUTPUT_FILE.getParent());

        try (CloseableIterator<String> results = transformed.executeAndCollect();
             PrintWriter writer = new PrintWriter(Files.newBufferedWriter(OUTPUT_FILE))) {

            writer.println("transactionId,customerId,product,quantity,unitPrice,totalPrice,category,date");

            results.forEachRemaining(writer::println);
        }

        System.out.println("Done. Output written to: " + OUTPUT_FILE.toAbsolutePath());
    }
}
