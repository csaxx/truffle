package org.csa.truffle.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.csa.truffle.graal.GraalPyInterpreter;
import org.csa.truffle.graal.PythonSourceFactory;
import org.csa.truffle.graal.reload.ScheduledReloader;
import org.csa.truffle.graal.source.PythonSourceConfig;
import org.csa.truffle.graal.source.ResourceSourceConfig;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    private final Duration reloadInterval;
    private final PythonSourceConfig sourceConfig;

    private transient GraalPyInterpreter interpreter;
    private transient ScheduledReloader reloader;
    private transient List<Map.Entry<String, Value>> pyProcessElements;
    private transient long lastGeneration = -1;

    public ProcessFunctionPython() {
        this(Duration.ofMinutes(5), new ResourceSourceConfig("python"));
    }

    public ProcessFunctionPython(Duration reloadInterval) {
        this(reloadInterval, new ResourceSourceConfig("python"));
    }

    public ProcessFunctionPython(Duration reloadInterval, PythonSourceConfig sourceConfig) {
        this.reloadInterval = reloadInterval;
        this.sourceConfig = sourceConfig;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        log.info("Opening: loading Python scripts from classpath 'python/' directory");
        interpreter = new GraalPyInterpreter(PythonSourceFactory.create(sourceConfig));
        reloader = new ScheduledReloader(interpreter, reloadInterval);
        reloader.start();   // synchronous initial reload + schedules background reloads
        pyProcessElements = interpreter.getNamedMembers("process_element");
        lastGeneration = interpreter.getGeneration();
        log.debug("Loaded {} process_element function(s)", pyProcessElements.size());
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing interpreter");
        if (reloader != null) reloader.close();
        if (interpreter != null) interpreter.close();
    }

    /**
     * Hot-reloads Python scripts from the production {@code python/} directory.
     */
    public void reload() throws IOException {
        log.info("Manual hot-reload triggered");
        if (interpreter.reload()) {
            log.info("Manual hot-reload complete: data changed");
        } else {
            log.debug("Manual hot-reload: no changes detected");
        }
    }

    @Override
    public void processElement(String line, Context ctx, Collector<String> out) {
        long gen = interpreter.getGeneration();
        if (gen != lastGeneration) {
            pyProcessElements = interpreter.getNamedMembers("process_element");
            lastGeneration = gen;
        }
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
