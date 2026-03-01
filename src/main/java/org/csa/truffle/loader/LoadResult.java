package org.csa.truffle.loader;

import java.util.List;

/**
 * Returned by {@link FileLoader#load()} and {@link FileLoader.FileLoadCallback}.
 * On success: {@code success} true, {@code changes} non-null, {@code error} null.
 * On failure: {@code success} false, {@code changes} null, {@code error} non-null.
 */
public record LoadResult(
        boolean success,
        FileLoaderStatus status,
        List<FileChangeInfo> changes,   // non-null on success (all files, any status)
        Exception error                 // non-null on failure
) {

    /**
     * Constructor (success).
     */
    public LoadResult(FileLoaderStatus status, List<FileChangeInfo> changes) {
        this(true, status, changes, null);
    }

    /**
     * Constructor (error).
     */
    public LoadResult(FileLoaderStatus status, Exception error) {
        this(false, status, null, error);
    }
}
