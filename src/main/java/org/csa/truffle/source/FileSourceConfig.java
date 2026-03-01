package org.csa.truffle.source;

import java.io.Serializable;

public interface FileSourceConfig extends Serializable {

    /** Glob pattern matched against the filename (last path component). {@code null} means no filter. */
    String filemask();

    /** Returns a copy of this config with the given filemask. */
    FileSourceConfig withFilemask(String filemask);
}
