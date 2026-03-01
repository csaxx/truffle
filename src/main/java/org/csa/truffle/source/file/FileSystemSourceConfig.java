package org.csa.truffle.source.file;

import org.csa.truffle.source.FileSourceConfig;

/**
 * @param directory absolute path to the local directory
 * @param watch     if true, starts a WatchService thread for push-notification hot reload
 * @param filemask  glob pattern matched against the filename; {@code null} means no filter
 */
public record FileSystemSourceConfig(String directory, boolean watch, String filemask)
        implements FileSourceConfig {

    public FileSystemSourceConfig(String directory, boolean watch) {
        this(directory, watch, null);
    }

    @Override
    public FileSourceConfig withFilemask(String filemask) {
        return new FileSystemSourceConfig(directory, watch, filemask);
    }
}
