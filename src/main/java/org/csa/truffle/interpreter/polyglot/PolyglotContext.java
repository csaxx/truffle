package org.csa.truffle.interpreter.polyglot;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

import java.util.*;

public class PolyglotContext implements AutoCloseable {

    private final TruffleLanguage language;
    private final String name;
    private final Context context;
    private final String contentHash;
    private final Value bindings;
    private final Map<String, Value> memberCache = new HashMap<>();

    public PolyglotContext(TruffleLanguage language, String name, Context context, String contentHash) {
        this.language = language;
        this.name = name;
        this.context = context;
        this.contentHash = contentHash;
        this.bindings = context.getBindings(language.getId());
        context.getBindings(language.getId()).getMemberKeys().forEach(m -> memberCache.put(m, null));
    }

    public TruffleLanguage language() {
        return language;
    }

    public String name() {
        return name;
    }

    public Context context() {
        return context;
    }

    public String contentHash() {
        return contentHash;
    }

    public Value getBindings() {
        return bindings;
    }

    public Set<String> getMembers() {
        return Collections.unmodifiableSet(memberCache.keySet());
    }

    public boolean hasMember(String memberName) {
        return memberCache.containsKey(memberName);
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

        Value member = bindings.getMember(memberName);

        if (member == null) {
            throw new NoSuchElementException(
                    "Member '" + memberName + "' not defined in '" + name + "'");
        }

        memberCache.put(memberName, member);

        return member;
    }

    @Override
    public void close() {
        memberCache.clear();
        context.close();
    }
}
