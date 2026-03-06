package org.csa.truffle.loader.result;

import org.csa.truffle.loader.FileLoader;
import org.csa.truffle.loader.FileLoaderStatus;

import java.util.List;
import java.util.Map;

/**
 * Returned by {@link FileLoader#load()} and {@link FileLoader.ReloadCallback}.
 * On success: {@code success} true, {@code changed} non-null, {@code files} non-null, {@code error} null.
 * On failure: {@code success} false, {@code files} null, {@code error} non-null.
 */
public record LoadResult(
        FileLoaderStatus status,
        boolean success,
        Boolean changed,              // non-null on success
        List<FileInfo> files,         // non-null on success (all files, any status)
        Map<String, String> contents, // non-null on success
        Exception error               // non-null on failure
) {

    /**
     * Constructor (success).
     */
    public static LoadResult forSuccess(FileLoaderStatus status, boolean changed, List<FileInfo> files, Map<String, String> contents) {
        return new LoadResult(status, true, changed, files, contents, null);
    }

    /**
     * Constructor (error).
     */
    public static LoadResult forError(FileLoaderStatus status, Exception error) {
        return new LoadResult(status, false, null, null, null, error);
    }

}
