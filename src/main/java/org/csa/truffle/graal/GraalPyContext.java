package org.csa.truffle.graal;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import java.util.HashMap;
import java.util.Map;

public class GraalPyContext {

    private final Context context;
    private final String name;
    private final Map<String, Value> memberCache = new HashMap<>();

    public GraalPyContext(Context context, String name) {
        this.context = context;
        this.name = name;
    }

    public Context context() { return context; }
    public String name()    { return name; }

    /**
     * Returns the cached {@link Value} for {@code memberName}, or {@code null}
     * if the Python module does not define that name.
     * The result is cached on first access.
     */
    public Value getMember(String memberName) {
        return memberCache.computeIfAbsent(memberName,
                k -> context.getBindings("python").getMember(k));
    }
}
