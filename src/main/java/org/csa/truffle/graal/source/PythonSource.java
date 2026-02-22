package org.csa.truffle.graal.source;

import org.csa.truffle.graal.GraalPyInterpreter;
import org.csa.truffle.graal.source.file.FilePythonSource;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Provides the file list and contents for {@link GraalPyInterpreter}.
 * Implementations may load from classpath resources, the filesystem,
 * a database, or an in-memory map.
 */
public interface PythonSource extends Closeable {

    /**
     * Returns the ordered list of Python filenames to load.
     */
    List<String> listFiles() throws IOException;

    /**
     * Returns the source code of the named file.
     */
    String readFile(String name) throws IOException;

    /**
     * Returns the most recent modification timestamp across all currently listed files,
     * or {@link Optional#empty()} if the source cannot determine this.
     * Called by {@link GraalPyInterpreter} at the end of each reload to populate
     * {@link org.csa.truffle.graal.reload.ReloadResult#dataAge()}.
     */
    default Optional<Instant> getDataAge() throws IOException {
        return Optional.empty();
    }

    /**
     * Called once by {@link GraalPyInterpreter} after construction.
     * Implementations that can detect changes (e.g. {@link FilePythonSource})
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
