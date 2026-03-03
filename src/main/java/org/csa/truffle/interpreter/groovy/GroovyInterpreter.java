package org.csa.truffle.interpreter.groovy;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import org.csa.truffle.interpreter.polyglot.PolyglotInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Manages the lifecycle of named, isolated Groovy script execution contexts.
 * <p>
 * Parallel to {@link PolyglotInterpreter}, adapted for Groovy's JVM embedding model.
 * Use the no-arg constructor and call {@link #addContext(String, String)} to load contexts.
 */
public class GroovyInterpreter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GroovyInterpreter.class);

    private GroovyShell shell;
    private final LinkedHashMap<String, GroovyScriptContext> contexts = new LinkedHashMap<>();

    /**
     * Creates an empty interpreter. Use {@link #addContext} to load contexts.
     */
    public GroovyInterpreter() {
        shell = new GroovyShell(new Binding());
    }

    /**
     * Compiles and evaluates {@code content} as a Groovy script identified by {@code name}.
     *
     * @param name    unique identifier for this context (e.g. filename)
     * @param content Groovy source code
     * @throws IllegalArgumentException  if a context with this name is already loaded
     * @throws groovy.lang.GroovyRuntimeException if the source fails to parse or evaluate
     */
    public void addContext(String name, String content) {
        if (contexts.containsKey(name)) {
            throw new IllegalArgumentException("Context already loaded: '" + name + "'");
        }
        Script script = shell.parse(content, name);
        script.run();
        contexts.put(name, new GroovyScriptContext(name, script));
        log.debug("Loaded context '{}'", name);
    }

    /**
     * Closes all contexts, clears the map, and recreates the shell to avoid classloader bloat.
     * The interpreter remains usable after this call; new contexts may be added via {@link #addContext}.
     */
    public void reset() {
        log.debug("Resetting interpreter: {} context(s)", contexts.size());
        contexts.values().forEach(ctx -> {
            try {
                ctx.close();
            } catch (Exception ignored) {
            }
        });
        contexts.clear();
        shell = new GroovyShell(new Binding());
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
     * Returns the {@link GroovyScriptContext} for the context name.
     *
     * @throws NoSuchElementException if the context is not loaded
     */
    public GroovyScriptContext getContext(String context) {
        if (!hasContext(context)) {
            throw new NoSuchElementException("Context '" + context + "' is not loaded");
        }
        return contexts.get(context);
    }

    /**
     * Returns whether the named context defines {@code member}.
     *
     * @throws NoSuchElementException if the context is not loaded
     */
    public boolean hasMember(String context, String member) {
        return getContext(context).hasMember(member);
    }

    /**
     * Returns all user-defined method names in the named context.
     *
     * @throws NoSuchElementException if the context is not loaded
     */
    public Set<String> getMemberNames(String context) {
        return getContext(context).getMembers();
    }

    /**
     * Returns the cached {@link GroovyCallable} for {@code member} in the named context.
     *
     * @throws NoSuchElementException if the context is not loaded or does not define the member
     */
    public GroovyCallable getMember(String context, String member) {
        return getContext(context).getMember(member);
    }

    /**
     * Returns one {@link GroovyCallable} per context in index order.
     *
     * @throws NoSuchElementException if any context does not define the member
     */
    public Map<String, GroovyCallable> getMembers(String member) {
        Map<String, GroovyCallable> members = new LinkedHashMap<>();
        for (Map.Entry<String, GroovyScriptContext> entry : contexts.entrySet()) {
            members.put(entry.getKey(), getMember(entry.getKey(), member));
        }
        return members;
    }

    /**
     * Returns whether {@code member} in the named context is callable.
     * All discovered members are methods and therefore always callable;
     * this returns {@code true} if the member exists.
     *
     * @throws NoSuchElementException if the context is not loaded or does not define the member
     */
    public boolean canExecute(String context, String member) {
        getMember(context, member);  // throws NoSuchElementException if absent
        return true;
    }

    /**
     * Executes {@code member} on the given context with {@code args}.
     *
     * @throws NoSuchElementException if the context is not loaded or does not define the member
     */
    public Object execute(String context, String member, Object... args) {
        return getMember(context, member).call(args);
    }

    /**
     * Executes {@code member} on the given context with {@code args}, ignoring the return value.
     *
     * @throws NoSuchElementException if the context is not loaded or does not define the member
     */
    public void executeVoid(String context, String member, Object... args) {
        getMember(context, member).call(args);
    }

    /**
     * Executes {@code member} on every loaded context in index order.
     *
     * @throws NoSuchElementException if any context does not define the member
     */
    public Map<String, Object> executeAll(String member, Object... args) {
        Map<String, Object> results = new LinkedHashMap<>();
        for (Map.Entry<String, GroovyScriptContext> entry : contexts.entrySet()) {
            results.put(entry.getKey(), getMember(entry.getKey(), member).call(args));
        }
        return results;
    }

    /**
     * Executes {@code member} on every loaded context in index order, ignoring return values.
     *
     * @throws NoSuchElementException if any context does not define the member
     */
    public void executeAllVoid(String member, Object... args) {
        for (Map.Entry<String, GroovyScriptContext> entry : contexts.entrySet()) {
            getMember(entry.getKey(), member).call(args);
        }
    }

    /**
     * Executes {@code member} on every context that defines it, skipping those that do not.
     * Returns results in index order (only from contexts where the member was present).
     */
    public Map<String, Object> executeAllPresent(String member, Object... args) {
        Map<String, Object> results = new LinkedHashMap<>();
        for (Map.Entry<String, GroovyScriptContext> entry : contexts.entrySet()) {
            if (entry.getValue().hasMember(member)) {
                results.put(entry.getKey(), getMember(entry.getKey(), member).call(args));
            }
        }
        return results;
    }

    /**
     * Executes {@code member} on every context that defines it, skipping those that do not.
     */
    public void executeAllVoidPresent(String member, Object... args) {
        for (Map.Entry<String, GroovyScriptContext> entry : contexts.entrySet()) {
            if (entry.getValue().hasMember(member)) {
                getMember(entry.getKey(), member).call(args);
            }
        }
    }

    @Override
    public void close() {
        reset();
    }
}
