package org.csa.truffle.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.csa.truffle.graal.reload.DatasetConfig;
import org.csa.truffle.graal.reload.ScheduledReloader;
import org.csa.truffle.graal.reload.SchedulerConfig;
import org.csa.truffle.graal.source.PythonSourceConfig;
import org.csa.truffle.graal.source.resource.ResourceSourceConfig;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

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

    private final List<DatasetConfig> datasetConfigs;
    private final String activeDatasetId;

    private transient ScheduledReloader reloader;
    private transient volatile List<Map.Entry<String, Value>> pyProcessElements;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /** Full multi-dataset constructor. */
    public ProcessFunctionPython(List<DatasetConfig> datasetConfigs, String activeDatasetId) {
        this.datasetConfigs  = List.copyOf(datasetConfigs);
        this.activeDatasetId = activeDatasetId;
    }

    /** Single-dataset convenience constructor. */
    public ProcessFunctionPython(DatasetConfig config) {
        this(List.of(config), config.id());
    }

    /** Backward-compat: uses classpath {@code python/} directory, 5-minute reload interval. */
    public ProcessFunctionPython() {
        this(new DatasetConfig("default",
                new ResourceSourceConfig("python"),
                new SchedulerConfig(Duration.ofMinutes(5))));
    }

    /** Backward-compat: uses classpath {@code python/} directory with a custom interval. */
    public ProcessFunctionPython(Duration interval) {
        this(new DatasetConfig("default",
                new ResourceSourceConfig("python"),
                new SchedulerConfig(interval)));
    }

    /** Backward-compat: explicit scheduler + source config, wrapped in a default dataset. */
    public ProcessFunctionPython(SchedulerConfig schedulerConfig, PythonSourceConfig sourceConfig) {
        this(new DatasetConfig("default", sourceConfig, schedulerConfig));
    }

    // -------------------------------------------------------------------------
    // Flink lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void open(OpenContext openContext) throws Exception {
        log.info("Opening: loading Python scripts");
        reloader = new ScheduledReloader();
        for (DatasetConfig cfg : datasetConfigs) {
            reloader.register(cfg.id(), cfg.sourceConfig(), cfg.schedulerConfig(),
                (datasetId, datasetStatus, interpreter) -> {
                    if (datasetId.equals(activeDatasetId)) {
                        pyProcessElements = interpreter.getNamedMembers("process_element");
                    }
                });
        }
        reloader.start();   // fires callback synchronously → pyProcessElements is set
        log.debug("Loaded {} process_element function(s)", pyProcessElements.size());
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing interpreter");
        if (reloader != null) reloader.close();   // also closes owned interpreters
    }

    // -------------------------------------------------------------------------
    // Processing
    // -------------------------------------------------------------------------

    @Override
    public void processElement(String line, Context ctx, Collector<String> out) {
        reloader.checkForFatalErrorById(activeDatasetId);
        for (Map.Entry<String, Value> entry : pyProcessElements) {
            try {
                entry.getValue().execute(line, out);
            } catch (PolyglotException e) {
                RuntimeException wrapped = new RuntimeException(
                        "Python error in '" + entry.getKey() + "' processing line: " + line, e);
                log.error("Python execution failed in file '{}': {}", entry.getKey(), e.getMessage(), wrapped);
            }
        }
    }
}
