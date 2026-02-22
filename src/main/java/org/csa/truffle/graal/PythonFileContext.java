package org.csa.truffle.graal;

import org.graalvm.polyglot.Context;

record PythonFileContext(Context context, String name, String hash) {
}
