package org.csa.truffle.graal.source;

import org.csa.truffle.graal.GraalPyInterpreter;

import java.io.IOException;
import java.util.List;

/**
 * Provides the file list and contents for {@link GraalPyInterpreter}.
 * Implementations may load from classpath resources, the filesystem,
 * a database, or an in-memory map.
 */
public interface PythonSource {

    /** Returns the ordered list of Python filenames to load. */
    List<String> listFiles() throws IOException;

    /** Returns the source code of the named file. */
    String readFile(String name) throws IOException;
}
