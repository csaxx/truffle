package org.csa.truffle.loader;

import java.time.Instant;
import java.util.Optional;

/**
 * Describes the change state of a single file after a {@link FileLoader#load()} call.
 *
 * @param filePath the key from {@code index.txt} (e.g. {@code "scripts/foo.py"} or {@code "foo.py"})
 * @param modifiedAt   mtime from {@link org.csa.truffle.source.FileSource}; empty when not available or REMOVED
 * @param status       change classification for this file
 */
public record FileChangeInfo(
        String filePath,
        Optional<Instant> modifiedAt,
        ChangeStatus status
) {

    public enum ChangeStatus {ADDED, UNMODIFIED, MODIFIED, REMOVED}

}
