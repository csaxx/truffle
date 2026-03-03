package org.csa.truffle.graal;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the lifecycle of per-file polyglot execution contexts.
 * <p>
 * Use the no-arg constructor and then call {@link #addFile(TruffleLanguage, String, String)}
 * to load files. All contexts share a per-language static {@link Engine} so compiled ASTs
 * are cached across contexts.
 */
public class GraalPyInterpreter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GraalPyInterpreter.class);

    /**
     * Per-language static engine cache — never closed; caches compiled ASTs across contexts.
     */
    private static final ConcurrentHashMap<TruffleLanguage, Engine> ENGINES = new ConcurrentHashMap<>();

    /**
     * Maps filename to context, in index order.
     */
    private final LinkedHashMap<String, GraalPyContext> fileContexts = new LinkedHashMap<>();

    /**
     * Creates an empty interpreter. Use {@link #addFile} to load files.
     */
    public GraalPyInterpreter() {
    }

    /**
     * Loads {@code content} as a polyglot source identified by {@code id}.
     *
     * @param language the language of the source
     * @param id       unique identifier for this file (e.g. filename)
     * @param content  source code
     * @throws IllegalArgumentException if a file with this id is already loaded
     * @throws Exception                if the source fails to evaluate
     */
    public void addFile(TruffleLanguage language, String id, String content) throws Exception {
        if (fileContexts.containsKey(id))
            throw new IllegalArgumentException("File already loaded: '" + id + "'");
        Context ctx = createContext(language);
        ctx.eval(Source.newBuilder(language.getId(), content, id).build());
        fileContexts.put(id, new GraalPyContext(ctx, id, language));
        log.debug("Loaded file '{}' ({})", id, language.getId());
    }

    /**
     * Closes all per-file contexts and clears the map.
     * The interpreter remains usable after this call; new files may be added via {@link #addFile}.
     */
    public void reset() {
        log.debug("Resetting interpreter: {} file context(s)", fileContexts.size());
        fileContexts.values().forEach(fc -> {
            try {
                fc.context().close();
            } catch (Exception ignored) {
            }
        });
        fileContexts.clear();
    }

    private Context createContext(TruffleLanguage language) {
        Engine engine = ENGINES.computeIfAbsent(language, lang -> Engine.newBuilder(lang.getId()).build());
        return Context.newBuilder(language.getId())
                .engine(engine)
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
     * Returns the cached {@link Value} for {@code memberName} in the named file.
     *
     * @throws NoSuchElementException if the file is not loaded or does not define the member
     */
    public Value getMember(String filename, String memberName) {
        GraalPyContext ctx = fileContexts.get(filename);
        if (ctx == null) throw new NoSuchElementException(
                "File '" + filename + "' is not loaded");
        return ctx.getMember(memberName);
    }

    /**
     * Returns one {@link Value} per file that defines {@code memberName},
     * in index order. Files that do not define the member are omitted.
     */
    public List<Value> getMembers(String memberName) {
        List<Value> result = new ArrayList<>();
        for (GraalPyContext fc : fileContexts.values()) {
            try {
                result.add(fc.getMember(memberName));
            } catch (NoSuchElementException ignored) {
            }
        }
        return List.copyOf(result);
    }

    /**
     * Executes {@code memberName} on the given file with {@code args}.
     * No-op if the file is not loaded or does not define the member.
     */
    public void execute(String filename, String memberName, Object... args) {
        try {
            getMember(filename, memberName).execute(args);
        } catch (NoSuchElementException ignored) {
        }
    }

    /**
     * Executes {@code memberName} on every loaded file that defines it, in index order.
     */
    public void executeAll(String memberName, Object... args) {
        for (GraalPyContext fc : fileContexts.values()) {
            try {
                fc.getMember(memberName).execute(args);
            } catch (NoSuchElementException ignored) {
            }
        }
    }

    @Override
    public void close() {
        reset();
    }
}
