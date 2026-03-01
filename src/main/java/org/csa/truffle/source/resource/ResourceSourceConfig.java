package org.csa.truffle.source.resource;

import org.csa.truffle.source.FileSourceConfig;

public record ResourceSourceConfig(String directory, String filemask) implements FileSourceConfig {

    public ResourceSourceConfig(String directory) {
        this(directory, null);
    }

    @Override
    public FileSourceConfig withFilemask(String filemask) {
        return new ResourceSourceConfig(directory, filemask);
    }
}
