package org.csa.truffle;

import org.csa.truffle.function.ProcessFunctionPython;
import org.csa.truffle.graal.reload.SchedulerConfig;
import org.csa.truffle.graal.source.resource.GitSourceConfig;
import org.csa.truffle.graal.source.resource.GitPythonSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;

/**
 * Git-backed variant of {@link TruffleJob}.
 * <p>
 * Loads Python transform scripts directly from GitHub at runtime via
 * {@link GitPythonSource}, runs them through a Flink pipeline, and writes
 * the result to {@code output/v3/sales_transformed.csv}.
 */
public class TruffleJobGit {

    private static final Logger log = LoggerFactory.getLogger(TruffleJobGit.class);

    public static void main(String[] args) throws Exception {
        GitSourceConfig gitConfig = new GitSourceConfig(
                "https://github.com/csaxx/truffle",
                "src/main/resources/python",
                "master",
                null,   // public repo — no token needed
                null    // auto-detect forge from URL
        );

        log.info("Loading CSV resources");
        List<String> allLines = TruffleJob.loadCsvLines();
        log.info("Loaded {} lines", allLines.size());

        log.info("Running V3 (Python + Git source) transform");
        List<String> v3 = TruffleJob.runTransform(
                allLines,
                new ProcessFunctionPython(new SchedulerConfig(Duration.ofMinutes(5)), gitConfig)
        );
        log.info("V3 complete: {} output rows", v3.size());
        TruffleJob.writeOutput(Paths.get("output", "v3", "sales_transformed.csv"), v3);

        log.info("Done. Output written to output/v3/");
    }
}
