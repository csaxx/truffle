package org.csa.truffle.interpreter.groovy;

/** Callable wrapper around a named Groovy script method; returned by getMember() and cached. */
@FunctionalInterface
public interface GroovyCallable {
    Object call(Object... args);
}
