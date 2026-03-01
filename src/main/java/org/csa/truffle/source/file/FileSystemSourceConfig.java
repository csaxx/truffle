package org.csa.truffle.source.file;

import org.csa.truffle.source.FileSourceConfig;

/**
 * @param directory absolute path to the local directory
 * @param watch     if true, starts a WatchService thread for push-notification hot reload
 */
public record FileSystemSourceConfig(String directory, boolean watch) implements FileSourceConfig {
}
