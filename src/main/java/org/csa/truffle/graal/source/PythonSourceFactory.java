package org.csa.truffle.graal.source;

import java.io.Serializable;

@FunctionalInterface
public interface PythonSourceFactory extends Serializable {
    PythonSource create();
}
