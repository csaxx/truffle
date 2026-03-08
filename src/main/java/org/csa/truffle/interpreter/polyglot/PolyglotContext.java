package org.csa.truffle.interpreter.polyglot;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class PolyglotContext implements AutoCloseable {

    private final Context context;
    private final String name;
    private final TruffleLanguage language;
    private final String contentHash;
    private final Map<String, Value> memberCache = new HashMap<>();

    public PolyglotContext(TruffleLanguage language, String name, Context context, String contentHash) {
        this.context = context;
        this.name = name;
        this.language = language;
        this.contentHash = contentHash;
    }

    public Context context() {
        return context;
    }

    public String name() {
        return name;
    }

    public TruffleLanguage language() {
        return language;
    }

    public String contentHash() {
        return contentHash;
    }

    public Set<String> getMembers() {
        return context.getBindings(language.getId()).getMemberKeys();
    }

    public boolean hasMember(String memberName) {
        if (memberCache.containsKey(memberName)) return true;
        Value v = context.getBindings(language.getId()).getMember(memberName);
        if (v != null) {
            memberCache.put(memberName, v);
            return true;
        }
        return false;
    }

    /**
     * Returns the cached {@link Value} for {@code memberName}.
     *
     * @throws NoSuchElementException if the module does not define that name
     */
    public Value getMember(String memberName) {

        Value cached = memberCache.get(memberName);
        if (cached != null) {
            return cached;
        }

        Value member = context.getBindings(language.getId()).getMember(memberName);
        if (member == null) {
            throw new NoSuchElementException(
                    "Member '" + memberName + "' not defined in '" + name + "'");
        }

        memberCache.put(memberName, member);

        return member;
    }

    /**
     * Invokes {@code memberName} as a method on the language bindings, passing the bindings as receiver.
     *
     * @throws NoSuchElementException if the member is not defined
     */
    public Value invokeMember(String memberName, Object... args) {
        return context.getBindings(language.getId()).invokeMember(memberName, args);
    }

    @Override
    public void close() {
        memberCache.clear();
        context.close();
    }
}
