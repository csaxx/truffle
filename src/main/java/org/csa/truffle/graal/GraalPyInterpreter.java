package org.csa.truffle.graal;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.Source;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    // Shared compilation engine — never closed; caches compiled ASTs across contexts.
    private static final Engine SHARED_ENGINE = Engine.newBuilder("python").build();

    // filename → SHA-256(name + content) at the time it was last eval'd
    private final Map<String, String> fileHashes = new LinkedHashMap<>();

    // volatile so getContext() sees the latest reference without synchronization
    private volatile Context context;

    public GraalPyInterpreter() throws IOException {
        this.context = createContext();
        loadAndCompileAll();
    }

    private Context createContext() {
        return Context.newBuilder("python")
                .engine(SHARED_ENGINE)
                .allowAllAccess(true)
                .build();
    }

    private void loadAndCompileAll() throws IOException {
        for (String name : readIndex()) {
            String path = PYTHON_DIR + "/" + name;
            String code = readResource(path);
            context.eval(Source.newBuilder("python", code, name).build());
            fileHashes.put(name, sha256(name, code));
        }
    }

    /**
     * Re-reads index.txt and re-evaluates any files whose content has changed.
     * If files were removed, the old Context is closed and a fresh one is created.
     *
     * @return {@code true} if anything changed (caller must re-fetch Value refs);
     *         {@code false} if nothing changed.
     */
    public synchronized boolean reload() throws IOException {
        List<String> currentNames = readIndex();
        Set<String> currentNameSet = new LinkedHashSet<>(currentNames);

        // Compute hashes and read contents for all current files
        Map<String, String> currentHashes = new LinkedHashMap<>();
        Map<String, String> currentContents = new LinkedHashMap<>();
        for (String name : currentNames) {
            String path = PYTHON_DIR + "/" + name;
            String code = readResource(path);
            currentHashes.put(name, sha256(name, code));
            currentContents.put(name, code);
        }

        // Files that need re-evaluation (new or changed)
        List<String> toEval = currentNames.stream()
                .filter(name -> !currentHashes.get(name).equals(fileHashes.get(name)))
                .toList();

        // Files that were removed
        Set<String> removed = new LinkedHashSet<>(fileHashes.keySet());
        removed.removeAll(currentNameSet);

        if (toEval.isEmpty() && removed.isEmpty()) {
            return false; // nothing changed
        }

        if (!removed.isEmpty()) {
            // Must recreate context since there's no way to delete bindings
            Context oldCtx = this.context;
            Context newCtx = createContext();
            for (String name : currentNames) {
                String code = currentContents.get(name);
                newCtx.eval(Source.newBuilder("python", code, name).build());
            }
            this.context = newCtx;
            oldCtx.close();
            fileHashes.clear();
            fileHashes.putAll(currentHashes);
        } else {
            // Only additions/changes: re-eval modified files in existing context
            for (String name : toEval) {
                String code = currentContents.get(name);
                context.eval(Source.newBuilder("python", code, name).build());
                fileHashes.put(name, currentHashes.get(name));
            }
        }

        return true;
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

    private static String sha256(String name, String content) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(name.getBytes(StandardCharsets.UTF_8));
            md.update((byte) 0); // separator avoids "a"+"bc" == "ab"+"c"
            md.update(content.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
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
