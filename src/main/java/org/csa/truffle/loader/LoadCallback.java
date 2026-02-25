package org.csa.truffle.loader;

import java.util.Map;

/**
 * Callback fired by {@link FileLoader#load()} after each attempt.
 * Exactly one method is called per {@code load()} invocation:
 * {@link #reloaded} on success, {@link #error} on failure.
 */
public interface LoadCallback {

    /** Called when {@code load()} succeeds. */
    void reloaded(Map<String, String> fileContents, LoadStatus status);

    /** Called when {@code load()} encounters an I/O error. */
    void error(LoadStatus status, Exception e);
}
