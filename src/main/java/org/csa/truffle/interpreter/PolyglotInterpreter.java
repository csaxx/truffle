package org.csa.truffle.interpreter;

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
 * Use the no-arg constructor and then call {@link #addContext(TruffleLanguage, String, String)}
 * to load contexts. All contexts share a per-language static {@link Engine} so compiled ASTs
 * are cached across contexts.
 */
public class PolyglotInterpreter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PolyglotInterpreter.class);

    /**
     * Per-language static engine cache — never closed; caches compiled ASTs across contexts.
     */
    private static final ConcurrentHashMap<TruffleLanguage, Engine> SHARED_ENGINES = new ConcurrentHashMap<>();

    public static void closeSharedEngines() {
        SHARED_ENGINES.values().forEach(Engine::close);
        SHARED_ENGINES.clear();
    }

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
     * Loads {@code content} as a polyglot source identified by {@code context}.
     *
     * @param language the language of the source
     * @param context  unique identifier for this context (e.g. filename)
     * @param content  source code
     * @throws IllegalArgumentException if a context with this context is already loaded
     * @throws Exception                if the source fails to evaluate
     */
    public void addContext(TruffleLanguage language, String context, String content) throws Exception {

        if (contexts.containsKey(context)) {
            throw new IllegalArgumentException("Context already loaded: '" + context + "'");
        }

        Context ctx = createContext(language);
        ctx.eval(Source.newBuilder(language.getId(), content, context).build());
        contexts.put(context, new PolyglotContext(language, context, ctx));

        log.debug("Loaded context '{}' ({})", context, language.getId());
    }

    /**
     * Closes all contexts and clears the map.
     * The interpreter remains usable after this call; new contexts may be added via {@link #addContext}.
     */
    public void reset() {
        log.debug("Resetting interpreter: {} context(s)", contexts.size());

        contexts.values().forEach(fc -> {
            try {
                fc.close();
            } catch (Exception ignored) {
            }
        });

        contexts.clear();
    }

    private Context createContext(TruffleLanguage language) {

        Engine engine = SHARED_ENGINES.computeIfAbsent(language, lang -> Engine.newBuilder(lang.getId()).build());

        return Context.newBuilder(language.getId())
                .engine(engine)
                .allowAllAccess(true)   // scripts are trusted internal transforms; full host access is intentional
                .build();
    }

    public boolean hasContext(String context) {
        return contexts.containsKey(context);
    }

    /**
     * Returns the names of all loaded contexts, in index order.
     */
    public List<String> getContexts() {
        return List.copyOf(contexts.keySet());
    }

    /**
     * Returns the {@link PolyglotContext} for the context name.
     *
     * @throws NoSuchElementException if the context is not loaded.
     */
    public PolyglotContext getContext(String context) {

        if (!hasContext(context)) {
            throw new NoSuchElementException("Context '" + context + "' is not loaded");
        }

        return contexts.get(context);
    }

    /**
     * Returns whether {@link PolyglotContext} for the context name has the {@code member}.
     *
     * @throws NoSuchElementException if the context is not loaded.
     */
    public boolean hasMember(String context, String member) throws NoSuchElementException {
        return getContext(context).hasMember(member);
    }

    /**
     * Returns all member names in the named context.
     *
     * @throws NoSuchElementException if the context is not loaded
     */
    public Set<String> getMemberNames(String context) throws NoSuchElementException {
        return getContext(context).getMembers();
    }

    /**
     * Returns the cached {@link Value} for {@code member} in the named context.
     *
     * @throws NoSuchElementException if the context is not loaded or does not define the member
     */
    public Value getMember(String context, String member) throws NoSuchElementException {
        return getContext(context).getMember(member);
    }

    /**
     * Returns one {@link Value} per context that defines {@code member}, in index order.
     *
     * @throws NoSuchElementException if the context is not loaded or does not define the member
     */
    public Map<String, Value> getMembers(String member) throws NoSuchElementException {

        Map<String, Value> members = new LinkedHashMap<>();

        for (Map.Entry<String, PolyglotContext> entry : contexts.entrySet()) {
            Value m = getMember(entry.getKey(), member);
            members.put(entry.getKey(), m);
        }

        return members;
    }

    /**
     * Returns whether {@code member} can be executed.
     *
     * @throws NoSuchElementException if the context is not loaded or does not define the member
     */
    public boolean canExecute(String context, String member) throws NoSuchElementException {
        return getMember(context, member).canExecute();
    }

    /**
     * Executes {@code member} on the given context with {@code args}.
     *
     * @throws NoSuchElementException if the context is not loaded or does not define the member
     */
    public Value execute(String context, String member, Object... args) throws NoSuchElementException {
        return getMember(context, member).execute(args);
    }

    /**
     * Executes {@code member} on the given context with {@code args}.
     *
     * @throws NoSuchElementException if the context is not loaded or does not define the member
     */
    public void executeVoid(String context, String member, Object... args) throws NoSuchElementException {
        getMember(context, member).executeVoid(args);
    }

    /**
     * Executes {@code member} on every loaded context that defines it, in index order.
     *
     * @throws NoSuchElementException if the context is not loaded or does not define the member
     */
    public Map<String, Value> executeAll(String member, Object... args) throws NoSuchElementException {

        Map<String, Value> results = new LinkedHashMap<>();

        for (Map.Entry<String, PolyglotContext> entry : contexts.entrySet()) {

            Value result = getMember(entry.getKey(), member).execute(args);
            results.put(entry.getKey(), result);
        }

        return results;
    }

    /**
     * Executes {@code member} on every loaded context that defines it, in index order.
     *
     * @throws NoSuchElementException if the context is not loaded or does not define the member
     */
    public void executeAllVoid(String member, Object... args) throws NoSuchElementException {
        for (Map.Entry<String, PolyglotContext> entry : contexts.entrySet()) {
            getMember(entry.getKey(), member).executeVoid(args);
        }
    }

    /**
     * Executes {@code member} on every loaded context that defines it, skipping those that do not.
     * Returns results in index order (only from contexts where the member was present).
     */
    public Map<String, Value> executeAllPresent(String member, Object... args) {
        Map<String, Value> results = new LinkedHashMap<>();
        for (Map.Entry<String, PolyglotContext> entry : contexts.entrySet()) {
            if (entry.getValue().hasMember(member)) {
                results.put(entry.getKey(), getMember(entry.getKey(), member).execute(args));
            }
        }
        return results;
    }

    /**
     * Executes {@code member} on every loaded context that defines it, skipping those that do not.
     */
    public void executeAllVoidPresent(String member, Object... args) {
        for (Map.Entry<String, PolyglotContext> entry : contexts.entrySet()) {
            if (entry.getValue().hasMember(member)) {
                getMember(entry.getKey(), member).executeVoid(args);
            }
        }
    }

    @Override
    public void close() {
        reset();
    }

}
