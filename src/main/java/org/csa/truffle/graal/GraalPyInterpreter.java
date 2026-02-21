package org.csa.truffle.graal;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Manages the lifecycle of per-file GraalPy execution contexts.
 * <p>
 * The caller must invoke {@link #reload(String)} before using the interpreter.
 * All contexts share the static {@code SHARED_ENGINE} so compiled ASTs are
 * reused across contexts.
 */
public class GraalPyInterpreter implements AutoCloseable {

    // Shared compilation engine — never closed; caches compiled ASTs across contexts.
    private static final Engine SHARED_ENGINE = Engine.newBuilder("python").build();

    private record FileContext(Context context, String hash) {
    }

    // insertion-ordered so getMembers() preserves index.txt order
    private final HashMap<String, FileContext> fileContexts = new LinkedHashMap<>();

    public GraalPyInterpreter() { }

    private Context createContext() {
        return Context.newBuilder("python")
                .engine(SHARED_ENGINE)
                .allowAllAccess(true)
                .build();
    }

    /**
     * Returns one {@link Value} per file for the given member name,
     * in index.txt order.
     */
    public List<Value> getMembers(String memberName) {
        return fileContexts.values().stream()
                .map(fc -> fc.context().getBindings("python").getMember(memberName))
                .toList();
    }

    /**
     * Returns the names of all currently loaded files, in index order.
     */
    public List<String> getLoadedFileNames() {
        return List.copyOf(fileContexts.keySet());
    }

    /**
     * Re-reads {@code directory/index.txt} and reconciles per-file contexts:
     * removed files get their Context closed, new/changed files get a fresh Context.
     *
     * @param directory classpath directory prefix (e.g. {@code "python"})
     * @return {@code true} if anything changed (caller must re-fetch Value refs);
     * {@code false} if nothing changed.
     */
    public synchronized boolean reload(String directory) throws IOException {
        List<String> currentNames = readIndex(directory);

        // Read contents and compute hashes for all current files
        Map<String, String> currentContents = new LinkedHashMap<>();
        Map<String, String> currentHashes = new LinkedHashMap<>();
        for (String name : currentNames) {
            String code = readResource(directory + "/" + name);
            currentContents.put(name, code);
            currentHashes.put(name, sha256(name, code));
        }

        boolean changed = false;

        // Close and remove contexts for files no longer in index
        List<String> removed = fileContexts.keySet().stream()
                .filter(name -> !currentHashes.containsKey(name))
                .toList();
        for (String name : removed) {
            fileContexts.remove(name).context().close();
            changed = true;
        }

        // Add or replace contexts for new/changed files
        for (String name : currentNames) {
            String newHash = currentHashes.get(name);
            FileContext existing = fileContexts.get(name);
            if (existing == null || !existing.hash().equals(newHash)) {
                if (existing != null) {
                    existing.context().close();
                }
                String code = currentContents.get(name);
                Context ctx = createContext();
                ctx.eval(Source.newBuilder("python", code, name).build());
                fileContexts.put(name, new FileContext(ctx, newHash));
                changed = true;
            }
        }

        if (!changed) {
            return false;
        }

        // Rebuild map in currentNames order (LinkedHashMap re-insertion)
        List<Map.Entry<String, FileContext>> entries = currentNames.stream()
                .filter(fileContexts::containsKey)
                .map(name -> Map.entry(name, fileContexts.get(name)))
                .toList();
        fileContexts.clear();
        entries.forEach(e -> fileContexts.put(e.getKey(), e.getValue()));

        return true;
    }

    private List<String> readIndex(String directory) throws IOException {
        return readResource(directory + "/index.txt").lines()
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

    @Override
    public void close() {
        fileContexts.values().forEach(fc -> fc.context().close());
    }
}
