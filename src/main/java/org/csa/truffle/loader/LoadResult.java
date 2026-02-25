package org.csa.truffle.loader;

import java.util.Map;

/**
 * Returned by {@link FileLoader#load()}.
 * On success: {@code success} true, {@code fileContents} non-null, {@code error} null.
 * On failure: {@code success} false, {@code fileContents} null, {@code error} non-null.
 */
public record LoadResult(
        boolean success,
        LoadStatus status,
        Map<String, String> fileContents,   // non-null on success
        Exception error                      // non-null on failure
) {}
