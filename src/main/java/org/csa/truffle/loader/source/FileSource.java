package org.csa.truffle.loader.source;

import org.csa.truffle.graal.GraalPyInterpreter;
import org.csa.truffle.loader.source.file.FileSystemSource;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Optional;

/**
 * Provides the file list and contents for {@link GraalPyInterpreter}.
 * Implementations may load from classpath resources, the filesystem,
 * a database, or an in-memory map.
 */
public interface FileSource extends Closeable {

    /**
     * Returns an ordered map of Python filenames to their modification timestamps.
     * Insertion order matches the execution order declared in {@code index.txt}.
     * The value is {@link Optional#empty()} if the source cannot determine the
     * modification time for a particular file.
     */
    LinkedHashMap<String, Optional<Instant>> listFiles() throws IOException;

    /**
     * Returns the source code of the named file.
     */
    String readFile(String name) throws IOException;

    /**
     * Called once by {@link GraalPyInterpreter} after construction.
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
