package org.csa.truffle.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.csa.truffle.graal.GraalPyInterpreter;
import org.graalvm.polyglot.Value;

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

    private transient GraalPyInterpreter interpreter;
    private transient List<Value> pyProcessElements;

    @Override
    public void open(OpenContext openContext) throws Exception {
        interpreter = new GraalPyInterpreter();
        pyProcessElements = interpreter.getMembers("process_element");
    }

    @Override
    public void close() throws Exception {
        if (interpreter != null) {
            interpreter.close();
        }
    }

    @Override
    public void processElement(String line, Context ctx, Collector<String> out) {
        for (Value fn : pyProcessElements) {
            fn.execute(line, out);
        }
    }

}
