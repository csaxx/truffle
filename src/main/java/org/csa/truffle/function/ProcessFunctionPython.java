package org.csa.truffle.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.csa.truffle.graal.GraalPyInterpreter;
import org.csa.truffle.source.FileSourceConfig;
import org.csa.truffle.source.resource.ResourceSourceConfig;
import org.csa.truffle.scheduler.ScheduledReloader;
import org.csa.truffle.scheduler.SchedulerConfig;
import org.graalvm.polyglot.PolyglotException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * V2 variant of {@link ProcessFunctionJava}.
 * <p>
 * Parses raw sales CSV lines and emits enriched records with a computed
 * totalPrice and a size category (small / medium / large).
 * <p>
 * Input  fields (6): transactionId, customerId, product, quantity, unitPrice, date
 * Output fields (8): transactionId, customerId, product, quantity, unitPrice,
 * totalPrice, category, date
 */
public class ProcessFunctionPython extends ProcessFunction<String, String> {

    private static final Logger log = LoggerFactory.getLogger(ProcessFunctionPython.class);

    private final FileSourceConfig sourceConfig;
    private final SchedulerConfig schedulerConfig;

    private transient ScheduledReloader scheduler;
    private transient GraalPyInterpreter interpreter;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Primary constructor.
     */
    public ProcessFunctionPython(FileSourceConfig sourceConfig, SchedulerConfig schedulerConfig) {
        this.sourceConfig = sourceConfig;
        this.schedulerConfig = schedulerConfig;
    }

    /**
     * Backward-compat: uses classpath {@code python/} directory, 5-minute reload interval.
     */
    public ProcessFunctionPython() {
        this(new ResourceSourceConfig("python"), new SchedulerConfig(Duration.ofMinutes(5)));
    }

    /**
     * Backward-compat: uses classpath {@code python/} directory with a custom interval.
     */
    public ProcessFunctionPython(Duration interval) {
        this(new ResourceSourceConfig("python"), new SchedulerConfig(interval));
    }

    /**
     * Backward-compat: explicit scheduler + source config, wrapped in a default dataset.
     */
    public ProcessFunctionPython(SchedulerConfig schedulerConfig, FileSourceConfig sourceConfig) {
        this(sourceConfig, schedulerConfig);
    }

    // -------------------------------------------------------------------------
    // Flink lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void open(OpenContext openContext) throws Exception {

        log.info("Opening: loading Python scripts");

        scheduler = new ScheduledReloader();
        scheduler.register("default", sourceConfig, schedulerConfig,
                (id, datasetStatus, newInterpreter) -> {
                    if (this.interpreter != null) {
                        this.interpreter.close();
                    }
                    this.interpreter = newInterpreter;
                });
        scheduler.start();   // fires callback synchronously → interpreter is set

        log.debug("Loaded {} process_element function(s)", interpreter.getLoadedFileNames().size());
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing interpreter");

        // also closes owned interpreters
        if (scheduler != null) {
            scheduler.close();
        }

        if (interpreter != null) {
            interpreter.close();
        }
    }

    // -------------------------------------------------------------------------
    // Processing
    // -------------------------------------------------------------------------

    @Override
    public void processElement(String line, Context ctx, Collector<String> out) {

        scheduler.checkForFatalError();

        for (String file : interpreter.getLoadedFileNames()) {
            try {
                interpreter.execute(file, "process_elements", line, out);
            } catch (PolyglotException e) {
                RuntimeException wrapped = new RuntimeException(
                        "Python error in '" + file + "' processing line: " + line, e);
                log.error("Python execution failed in file '{}': {}", file, e.getMessage(), wrapped);
            }
        }
    }
}
