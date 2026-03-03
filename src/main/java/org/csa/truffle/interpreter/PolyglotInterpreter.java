package org.csa.truffle.interpreter;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the lifecycle of per-file polyglot execution contexts.
 * <p>
 * Use the no-arg constructor and then call {@link #addContext(TruffleLanguage, String, String)}
 * to load contexts. All contexts share a per-language static {@link Engine} so compiled ASTs
 * are cached across contexts.
 */
public class PolyglotInterpreter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PolyglotInterpreter.class);

    /**
     * Per-language static engine cache — never closed; caches compiled ASTs across contexts.
     */
    private static final ConcurrentHashMap<TruffleLanguage, Engine> ENGINES = new ConcurrentHashMap<>();

    /**
     * Maps name to context, in index order.
     */
    private final LinkedHashMap<String, PolyglotContext> contexts = new LinkedHashMap<>();

    /**
     * Creates an empty interpreter. Use {@link #addContext} to load contexts.
     */
    public PolyglotInterpreter() {
    }

    /**
     * Loads {@code content} as a polyglot source identified by {@code name}.
     *
     * @param language the language of the source
     * @param name       unique identifier for this context (e.g. filename)
     * @param content  source code
     * @throws IllegalArgumentException if a context with this name is already loaded
     * @throws Exception                if the source fails to evaluate
     */
    public void addContext(TruffleLanguage language, String name, String content) throws Exception {

        if (contexts.containsKey(name)) {
            throw new IllegalArgumentException("Context already loaded: '" + name + "'");
        }

        Context ctx = createContext(language);
        ctx.eval(Source.newBuilder(language.getId(), content, name).build());
        contexts.put(name, new PolyglotContext(language, name, ctx));

        log.debug("Loaded context '{}' ({})", name, language.getId());
    }

    /**
     * Closes all contexts and clears the map.
     * The interpreter remains usable after this call; new contexts may be added via {@link #addContext}.
     */
    public void reset() {
        log.debug("Resetting interpreter: {} context(s)", contexts.size());

        contexts.values().forEach(fc -> {
            try {
                fc.context().close();
            } catch (Exception ignored) {
            }
        });

        contexts.clear();
    }

    private Context createContext(TruffleLanguage language) {

        Engine engine = ENGINES.computeIfAbsent(language, lang -> Engine.newBuilder(lang.getId()).build());

        return Context.newBuilder(language.getId())
                .engine(engine)
                .allowAllAccess(true)
                .build();
    }

    /**
     * Returns the names of all loaded contexts, in index order.
     */
    public List<String> getLoadedNames() {
        return List.copyOf(contexts.keySet());
    }

    /**
     * Returns the cached {@link Value} for {@code memberName} in the named context.
     *
     * @throws NoSuchElementException if the context is not loaded or does not define the member
     */
    public Value getMember(String name, String memberName) {

        PolyglotContext ctx = contexts.get(name);

        if (ctx == null) {
            throw new NoSuchElementException(
                    "Context '" + name + "' is not loaded");
        }

        return ctx.getMember(memberName);
    }

    /**
     * Returns one {@link Value} per context that defines {@code memberName},
     * in index order. Contexts that do not define the member are omitted.
     */
    public List<Value> getMembers(String memberName) {
        List<Value> result = new ArrayList<>();

        for (PolyglotContext fc : contexts.values()) {
            try {
                result.add(fc.getMember(memberName));
            } catch (NoSuchElementException ignored) {
            }
        }

        return List.copyOf(result);
    }

    /**
     * Executes {@code memberName} on the given context with {@code args}.
     * No-op if the context is not loaded or does not define the member.
     */
    public void execute(String name, String memberName, Object... args) {
        try {
            getMember(name, memberName).execute(args);
        } catch (NoSuchElementException ignored) {
        }
    }

    /**
     * Executes {@code memberName} on every loaded context that defines it, in index order.
     */
    public void executeAll(String memberName, Object... args) {
        for (PolyglotContext fc : contexts.values()) {
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
