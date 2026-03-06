package org.csa.truffle.source.file;

import org.csa.truffle.source.FileSourceConfig;

/**
 * @param directory        absolute path to the local directory
 * @param watch            if true, starts a WatchService thread for push-notification hot reload
 * @param filemasks        glob patterns matched against the filename; {@code null} or empty means no filter
 * @param excludeFilemasks glob patterns matched against each path component; a file is excluded if any
 *                         pattern matches any component; {@code null} means no exclusions
 */
public record FileSystemSourceConfig(String directory, boolean watch, String[] filemasks, String[] excludeFilemasks)
        implements FileSourceConfig {

    public FileSystemSourceConfig(String directory, boolean watch) {
        this(directory, watch, null, null);
    }
}
