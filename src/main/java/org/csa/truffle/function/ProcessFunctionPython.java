package org.csa.truffle.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.csa.truffle.graal.GraalPyInterpreter;

import java.util.Locale;

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

    @Override
    public void open(OpenContext openContext) throws Exception {
        interpreter = new GraalPyInterpreter();
    }

    @Override
    public void close() throws Exception {
        if (interpreter != null) {
            interpreter.close();
        }
    }

    @Override
    public void processElement(String line, Context ctx, Collector<String> out) {
        // Skip header rows and blank lines
        if (line.isBlank() || line.startsWith("transactionId")) {
            return;
        }

        String[] f = line.split(",", -1);
        if (f.length != 6) {
            return;
        }

        try {
            String transactionId = f[0].trim();
            String customerId    = f[1].trim();
            String product       = f[2].trim();
            int    quantity      = Integer.parseInt(f[3].trim());
            double unitPrice     = Double.parseDouble(f[4].trim());
            String date          = f[5].trim();

            double totalPrice = quantity * unitPrice;

            String category;
            if (totalPrice < 100.0) {
                category = "small";
            } else if (totalPrice < 500.0) {
                category = "medium";
            } else {
                category = "large";
            }

            out.collect(String.join(",",
                    transactionId,
                    customerId,
                    product,
                    String.valueOf(quantity),
                    String.format(Locale.US, "%.2f", unitPrice),
                    String.format(Locale.US, "%.2f", totalPrice),
                    category,
                    date));

        } catch (NumberFormatException ignored) {
            // Drop records with unparseable numeric fields
        }
    }
}
