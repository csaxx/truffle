package org.csa.truffle.source;

import java.io.Serializable;

public interface FileSourceConfig extends Serializable {

    /** Glob patterns matched against the filename (last path component). A file matches if it matches
     * any pattern. {@code null} or empty array means no filter (accept all). */
    String[] filemasks();
}
