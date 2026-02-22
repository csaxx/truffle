package org.csa.truffle.graal.source;

/**
 * @param directory absolute path to the local directory
 * @param watch     if true, starts a WatchService thread for push-notification hot reload
 */
public record FileSourceConfig(String directory, boolean watch) implements PythonSourceConfig {
}
