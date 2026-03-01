package org.csa.truffle.graal;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class GraalPyContext {

    private final Context context;
    private final String name;
    private final Map<String, Value> memberCache = new HashMap<>();

    public GraalPyContext(Context context, String name) {
        this.context = context;
        this.name = name;
    }

    public Context context() {
        return context;
    }

    public String name() {
        return name;
    }

    /**
     * Returns the cached {@link Value} for {@code memberName}.
     *
     * @throws NoSuchElementException if the Python module does not define that name
     */
    public Value getMember(String memberName) {
        Value cached = memberCache.get(memberName);
        if (cached != null) return cached;
        Value member = context.getBindings("python").getMember(memberName);
        if (member == null) throw new NoSuchElementException(
                "Python member '" + memberName + "' not defined in '" + name + "'");
        memberCache.put(memberName, member);
        return member;
    }
}
