package org.csa.truffle.loader.source.file;

import org.csa.truffle.loader.source.FileSourceConfig;

/**
 * @param directory absolute path to the local directory
 * @param watch     if true, starts a WatchService thread for push-notification hot reload
 */
public record FileSystemSourceConfig(String directory, boolean watch) implements FileSourceConfig {
}
