package org.csa.truffle.graal;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Manages the lifecycle of per-file GraalPy execution contexts.
 * <p>
 * Constructed from a {@code Map<String, String>} of filename → Python source.
 * All contexts share the static {@code SHARED_ENGINE} so compiled ASTs are
 * reused across contexts.
 */
public class GraalPyInterpreter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GraalPyInterpreter.class);

    /**
     * Shared compilation engine — never closed; caches compiled ASTs across contexts.
     */
    private static final Engine SHARED_ENGINE = Engine.newBuilder("python").build();

    /**
     * Maps filename to GraalPy context, in index order.
     */
    private final HashMap<String, GraalPyContext> fileContexts = new LinkedHashMap<>();

    /**
     * Creates an interpreter pre-loaded from {@code fileContents}.
     * Iteration order of the map determines index order; pass a
     * {@link java.util.LinkedHashMap} for deterministic ordering.
     *
     * @param fileContents filename → Python source code
     */
    public GraalPyInterpreter(Map<String, String> fileContents) throws Exception {
        log.debug("Initializing with {} file(s)", fileContents.size());
        for (Map.Entry<String, String> entry : fileContents.entrySet()) {
            String name = entry.getKey();
            String code = entry.getValue();
            Context ctx = createContext();
            ctx.eval(Source.newBuilder("python", code, name).build());
            fileContexts.put(name, new GraalPyContext(ctx, name));
        }
        log.debug("Initialized: {} context(s) created", fileContexts.size());
    }

    private Context createContext() {
        return Context.newBuilder("python")
                .engine(SHARED_ENGINE)
                .allowAllAccess(true)
                .build();
    }

    /**
     * Returns the names of all loaded files, in index order.
     */
    public List<String> getLoadedFileNames() {
        return List.copyOf(fileContexts.keySet());
    }

    /**
     * Returns the cached {@link Value} for {@code memberName} in the named file,
     * or {@code null} if the file is not loaded or does not define that member.
     */
    public Value getMember(String filename, String memberName) {
        GraalPyContext ctx = fileContexts.get(filename);
        return ctx == null ? null : ctx.getMember(memberName);
    }

    /**
     * Returns one {@link Value} per file that defines {@code memberName},
     * in index order. Files that do not define the member are omitted.
     */
    public List<Value> getMembers(String memberName) {
        return fileContexts.values().stream()
                .map(fc -> fc.getMember(memberName))
                .filter(Objects::nonNull)
                .toList();
    }

    /**
     * Executes {@code memberName} on the given file with {@code args}.
     * No-op if the file is not loaded or does not define the member.
     */
    public void execute(String filename, String memberName, Object... args) {
        Value fn = getMember(filename, memberName);
        if (fn != null) fn.execute(args);
    }

    /**
     * Executes {@code memberName} on every loaded file that defines it, in index order.
     */
    public void executeAll(String memberName, Object... args) {
        fileContexts.values().stream()
                .map(fc -> fc.getMember(memberName))
                .filter(Objects::nonNull)
                .forEach(fn -> fn.execute(args));
    }

    @Override
    public void close() {

        log.debug("Closing interpreter: {} file context(s)", fileContexts.size());

        fileContexts.values().forEach(fc -> {
            try {
                fc.context().close();
            } catch (Exception ignored) {
            }
        });
    }
}
