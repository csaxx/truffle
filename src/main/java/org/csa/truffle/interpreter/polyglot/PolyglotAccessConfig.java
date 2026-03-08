package org.csa.truffle.interpreter.polyglot;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotAccess;
import org.graalvm.polyglot.io.IOAccess;

import java.io.Serializable;

/**
 * Configures GraalVM context permissions for {@link PolyglotInterpreter}.
 * <p>
 * Use one of the predefined constants ({@link #MINIMAL}, {@link #FULL}, {@link #SANDBOXED})
 * or construct a custom instance. Pass to {@link PolyglotInterpreter#PolyglotInterpreter(PolyglotAccessConfig)}.
 */
public record PolyglotAccessConfig(
        HostAccessMode hostAccess,
        boolean allowHostClassLookup,
        IOAccessMode ioAccess,
        boolean allowNativeAccess,
        boolean allowCreateThread,
        PolyglotAccessMode polyglotAccess
) implements Serializable {

    /** Minimal: host objects only (out.collect()), deny class lookup / IO / native / threads / polyglot. */
    public static final PolyglotAccessConfig MINIMAL = new PolyglotAccessConfig(
            HostAccessMode.ALL, false, IOAccessMode.NONE, false, false, PolyglotAccessMode.NONE);

    /** Full: all permissions enabled. */
    public static final PolyglotAccessConfig FULL = new PolyglotAccessConfig(
            HostAccessMode.ALL, true, IOAccessMode.ALL, true, true, PolyglotAccessMode.ALL);

    /** Sandboxed: no host access, no I/O — pure language sandbox. */
    public static final PolyglotAccessConfig SANDBOXED = new PolyglotAccessConfig(
            HostAccessMode.NONE, false, IOAccessMode.NONE, false, false, PolyglotAccessMode.NONE);

    /**
     * Applies this configuration to a {@link Context.Builder} and returns it.
     */
    public Context.Builder applyTo(Context.Builder builder) {
        HostAccess ha = switch (hostAccess) {
            case ALL -> HostAccess.ALL;
            case EXPLICIT -> HostAccess.EXPLICIT;
            case NONE -> HostAccess.NONE;
        };
        IOAccess io = switch (ioAccess) {
            case ALL -> IOAccess.ALL;
            case NONE -> IOAccess.NONE;
        };
        PolyglotAccess pa = switch (polyglotAccess) {
            case ALL -> PolyglotAccess.ALL;
            case NONE -> PolyglotAccess.NONE;
        };
        return builder
                .allowHostAccess(ha)
                .allowHostClassLookup(allowHostClassLookup ? s -> true : s -> false)
                .allowIO(io)
                .allowNativeAccess(allowNativeAccess)
                .allowCreateThread(allowCreateThread)
                .allowPolyglotAccess(pa);
    }

    public enum HostAccessMode { ALL, EXPLICIT, NONE }

    public enum IOAccessMode { ALL, NONE }

    public enum PolyglotAccessMode { ALL, NONE }
}
