package org.csa.truffle;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.CloseableIterator;
import org.csa.truffle.function.ProcessFunctionJava;
import org.csa.truffle.function.ProcessFunctionPython;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Truffle Flink job.
 * <p>
 * Reads three quarterly sales CSV files from the classpath, applies
 * {@link ProcessFunctionJava} (V1) and {@link ProcessFunctionPython} (V2)
 * to enrich each record, then writes the results to separate subdirectories
 * under {@code output/}.
 * <p>
 * Run locally:
 * mvn exec:java
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
        writeOutput(Paths.get("output", "java", "sales_transformed.csv"), v1);

        log.info("Running V2 (Python) transform");
        List<String> v2 = runTransform(allLines, new ProcessFunctionPython());
        log.info("V2 complete: {} output rows", v2.size());
        writeOutput(Paths.get("output", "python", "sales_transformed.csv"), v2);

        log.info("Done. Output written to output/java/ and output/python/");
    }

    static List<String> loadCsvLines() throws Exception {
        List<String> lines = new ArrayList<>();
        for (String resource : CSV_RESOURCES) {
            try (InputStream is = TruffleJob.class.getClassLoader().getResourceAsStream(resource)) {
                lines.addAll(IOUtils.readLines(is, StandardCharsets.UTF_8));
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

    static void writeOutput(Path file, List<String> lines) throws Exception {
        List<String> output = new ArrayList<>();
        output.add("transactionId,customerId,product,quantity,unitPrice,totalPrice,category,date");
        output.addAll(lines);
        FileUtils.writeLines(file.toFile(), StandardCharsets.UTF_8.name(), output);
    }
}
