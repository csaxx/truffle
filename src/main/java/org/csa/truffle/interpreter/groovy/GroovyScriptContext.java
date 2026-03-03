package org.csa.truffle.interpreter.groovy;

import groovy.lang.Script;
import org.csa.truffle.interpreter.polyglot.PolyglotContext;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

/** Wraps a compiled Groovy {@link Script} with a member cache. Parallel to {@link PolyglotContext}. */
public class GroovyScriptContext implements AutoCloseable {

    private final Script script;
    private final String name;
    private final Map<String, GroovyCallable> memberCache = new HashMap<>();

    public GroovyScriptContext(String name, Script script) {
        this.name = name;
        this.script = script;
    }

    public Script script() {
        return script;
    }

    public String name() {
        return name;
    }

    /**
     * Returns the names of all user-defined methods in this script, excluding compiler-generated
     * helpers, the {@code run} body method, and synthetic/bridge methods.
     */
    public Set<String> getMembers() {
        return Arrays.stream(script.getClass().getDeclaredMethods())
                .filter(m -> Modifier.isPublic(m.getModifiers()))
                .filter(m -> !m.isSynthetic())
                .filter(m -> !m.isBridge())
                .filter(m -> !m.getName().equals("run"))
                .filter(m -> !m.getName().contains("$"))
                .map(Method::getName)
                .collect(Collectors.toSet());
    }

    private boolean isUserDefinedMethod(String memberName) {
        return Arrays.stream(script.getClass().getDeclaredMethods())
                .filter(m -> Modifier.isPublic(m.getModifiers()))
                .filter(m -> !m.isSynthetic())
                .filter(m -> !m.isBridge())
                .filter(m -> !m.getName().equals("run"))
                .filter(m -> !m.getName().contains("$"))
                .anyMatch(m -> m.getName().equals(memberName));
    }

    public boolean hasMember(String memberName) {
        if (memberCache.containsKey(memberName)) return true;
        if (isUserDefinedMethod(memberName)) {
            memberCache.put(memberName, args -> script.invokeMethod(memberName, args));
            return true;
        }
        return false;
    }

    /**
     * Returns the cached {@link GroovyCallable} for {@code memberName}.
     *
     * @throws NoSuchElementException if the script does not define that method
     */
    public GroovyCallable getMember(String memberName) {
        GroovyCallable cached = memberCache.get(memberName);
        if (cached != null) {
            return cached;
        }
        if (!isUserDefinedMethod(memberName)) {
            throw new NoSuchElementException(
                    "Member '" + memberName + "' not defined in '" + name + "'");
        }
        GroovyCallable callable = args -> script.invokeMethod(memberName, args);
        memberCache.put(memberName, callable);
        return callable;
    }

    @Override
    public void close() {
        memberCache.clear();
    }
}
