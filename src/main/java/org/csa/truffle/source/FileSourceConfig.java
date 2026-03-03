package org.csa.truffle.source;

import java.io.Serializable;

public interface FileSourceConfig extends Serializable {

    /** Glob patterns matched against the filename (last path component). A file matches if it matches
     * any pattern. {@code null} or empty array means no filter (accept all). */
    String[] filemasks();

    /** Glob patterns matched against each path component of a relative file path. A file is excluded
     * if any pattern matches any component. {@code null} means no exclusions. */
    default String[] excludeFilemasks() { return null; }
}
