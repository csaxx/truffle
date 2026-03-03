package org.csa.truffle.loader;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Returned by {@link FileLoader#load()} and {@link FileLoader.ReloadCallback}.
 * On success: {@code success} true, {@code changes} non-null, {@code error} null.
 * On failure: {@code success} false, {@code changes} null, {@code error} non-null.
 */
public record LoadResult(
        boolean success,
        FileLoaderStatus status,
        List<FileChangeInfo> changes,   // non-null on success (all files, any status)
        Map<String, String> contents,
        Exception error                 // non-null on failure
) {

    public enum ChangeStatus {ADDED, UNMODIFIED, MODIFIED, REMOVED}

    /**
     * Describes the change state of a single file after a {@link FileLoader#load()} call.
     *
     * @param filePath   the key from {@code index.txt} (e.g. {@code "scripts/foo.py"} or {@code "foo.py"})
     * @param modifiedAt mtime from {@link org.csa.truffle.source.FileSource}; empty when not available or REMOVED
     * @param status     change classification for this file
     */
    public record FileChangeInfo(
            String filePath,
            Optional<Instant> modifiedAt,
            ChangeStatus status
    ) {
    }

    /**
     * Constructor (success).
     */
    public LoadResult(FileLoaderStatus status, List<FileChangeInfo> changes, Map<String, String> contents) {
        this(true, status, changes, contents, null);
    }

    /**
     * Constructor (error).
     */
    public LoadResult(FileLoaderStatus status, Exception error) {
        this(false, status, null, null, error);
    }

}
