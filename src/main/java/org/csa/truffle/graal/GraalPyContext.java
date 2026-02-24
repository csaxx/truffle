package org.csa.truffle.graal;

import org.graalvm.polyglot.Context;

public record GraalPyContext(Context context, String name, String hash) {
}
