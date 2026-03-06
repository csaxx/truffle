package org.csa.truffle.source.resource;

import org.csa.truffle.source.FileSourceConfig;

public record ResourceSourceConfig(String directory, String[] filemasks, String[] excludeFilemasks)
        implements FileSourceConfig {

    public ResourceSourceConfig(String directory) {
        this(directory, null, null);
    }
}
