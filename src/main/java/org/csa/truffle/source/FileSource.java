package org.csa.truffle.source;

import org.csa.truffle.graal.GraalPyInterpreter;
import org.csa.truffle.source.file.FileSystemSource;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Provides the file list and contents from a file source.
 * Implementations may load from classpath resources, the filesystem,
 * a database, or an in-memory map.
 */
public interface FileSource extends Closeable {

    /**
     * Returns an ordered map of  filenames to their modification timestamps.
     * Insertion order matches the execution order declared in {@code index.txt}.
     * The value is {@link Optional#empty()} if the source cannot determine the
     * modification time for a particular file.
     */
    Map<String, Optional<Instant>> listFiles() throws IOException;

    /**
     * Returns the source code of the named file.
     */
    String readFile(String name) throws IOException;

    /**
     * Must be called after construction.
     * Implementations that can detect changes (e.g. {@link FileSystemSource})
     * store the callback and invoke it when a change is detected.
     * The default is a no-op (pull-only sources ignore it).
     */
    default void setChangeListener(Runnable onChanged) {
    }

    /**
     * Releases any resources held (e.g. a watcher thread). No-op by default.
     */
    @Override
    default void close() throws IOException {
    }
}
