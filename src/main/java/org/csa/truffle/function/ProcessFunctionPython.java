package org.csa.truffle.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.csa.truffle.graal.GraalPyInterpreter;
import org.csa.truffle.graal.source.ResourcePythonSource;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * V2 variant of {@link ProcessFunctionJava}.
 *
 * Parses raw sales CSV lines and emits enriched records with a computed
 * totalPrice and a size category (small / medium / large).
 *
 * Input  fields (6): transactionId, customerId, product, quantity, unitPrice, date
 * Output fields (8): transactionId, customerId, product, quantity, unitPrice,
 *                    totalPrice, category, date
 */
public class ProcessFunctionPython extends ProcessFunction<String, String> {

    private static final Logger log = LoggerFactory.getLogger(ProcessFunctionPython.class);

    private transient GraalPyInterpreter interpreter;
    private transient List<Value> pyProcessElements;
    private transient long lastGeneration = -1;

    @Override
    public void open(OpenContext openContext) throws Exception {
        log.info("Opening: loading Python scripts from classpath 'python/' directory");
        interpreter = new GraalPyInterpreter(new ResourcePythonSource("python"));
        interpreter.reload();
        pyProcessElements = interpreter.getMembers("process_element");
        lastGeneration = interpreter.getGeneration();
        log.debug("Loaded {} process_element function(s)", pyProcessElements.size());
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing interpreter");
        if (interpreter != null) {
            interpreter.close();
        }
    }

    /** Hot-reloads Python scripts from the production {@code python/} directory. */
    public void reload() throws IOException {
        log.info("Hot-reload triggered");
        if (interpreter.reload()) {
            pyProcessElements = interpreter.getMembers("process_element");
            lastGeneration = interpreter.getGeneration();
            log.info("Hot-reload complete: {} process_element function(s) active",
                     pyProcessElements.size());
        } else {
            log.debug("Hot-reload: no changes detected");
        }
    }

    @Override
    public void processElement(String line, Context ctx, Collector<String> out) {
        long gen = interpreter.getGeneration();
        if (gen != lastGeneration) {
            pyProcessElements = interpreter.getMembers("process_element");
            lastGeneration = gen;
        }
        for (Value fn : pyProcessElements) {
            fn.execute(line, out);
        }
    }

}
