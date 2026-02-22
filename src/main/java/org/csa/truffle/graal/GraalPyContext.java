package org.csa.truffle.graal;

import org.graalvm.polyglot.Context;

record GraalPyContext(Context context, String name, String hash) {
}
