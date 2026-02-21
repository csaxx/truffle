package org.csa.truffle.graal;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Manages the lifecycle of a GraalPy execution context.
 *
 * On construction the interpreter reads {@code python/index.txt} from the
 * classpath, then loads and compiles every {@code .py} file listed there so
 * that their top-level definitions are available in the shared context.
 */
public class GraalPyInterpreter implements AutoCloseable {

    private static final String PYTHON_DIR = "python";
    private static final String INDEX = PYTHON_DIR + "/index.txt";

    private final Context context;

    public GraalPyInterpreter() throws IOException {
        this.context = Context.newBuilder("python")
                .allowAllAccess(true)
                .build();
        loadAndCompileAll();
    }

    private void loadAndCompileAll() throws IOException {
        for (String name : readIndex()) {
            String path = PYTHON_DIR + "/" + name;
            String code = readResource(path);
            context.eval(Source.newBuilder("python", code, name).build());
        }
    }

    private List<String> readIndex() throws IOException {
        return readResource(INDEX).lines()
                .map(String::trim)
                .filter(l -> !l.isEmpty() && !l.startsWith("#"))
                .toList();
    }

    private String readResource(String path) throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            if (is == null) throw new IOException("Resource not found: " + path);
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** Returns the underlying GraalPy context for direct interaction. */
    public Context getContext() {
        return context;
    }

    @Override
    public void close() {
        context.close();
    }
}
