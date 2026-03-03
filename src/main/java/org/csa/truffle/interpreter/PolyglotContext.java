package org.csa.truffle.interpreter;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class PolyglotContext {

    private final Context context;
    private final String name;
    private final TruffleLanguage language;
    private final Map<String, Value> memberCache = new HashMap<>();

    public PolyglotContext(TruffleLanguage language, String name, Context context) {
        this.context = context;
        this.name = name;
        this.language = language;
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

    /**
     * Returns the cached {@link Value} for {@code memberName}.
     *
     * @throws NoSuchElementException if the module does not define that name
     */
    public Value getMember(String memberName) {
        Value cached = memberCache.get(memberName);
        if (cached != null) return cached;
        Value member = context.getBindings(language.getId()).getMember(memberName);
        if (member == null) throw new NoSuchElementException(
                "Member '" + memberName + "' not defined in '" + name + "'");
        memberCache.put(memberName, member);
        return member;
    }
}
