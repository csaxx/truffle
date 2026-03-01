package org.csa.truffle.loader;

import java.time.Instant;
import java.util.Set;

/**
 * Observable status for a {@link FileLoader}.
 *
 * <p>Fields are written by {@link FileLoader} (package-private access)
 * after every {@link FileLoader#load()} call.
 * All getters are public for external observation.
 */
public class FileLoaderStatus {

    /**
     * Wall-clock time of the most recent {@code load()} call; {@code null} until the first call.
     */
    Instant lastCheckedAt;

    /**
     * Wall-clock time of the most recent load that detected a change; {@code null} until then.
     */
    Instant lastChangedAt;

    /**
     * Wall-clock time of the most recent successful attempt to load the new files; {@code null} until then.
     */
    Instant lastSuccessAt;

    /**
     * Max modification time across all files from the most recent {@code load()}; {@code null} when none reported.
     */
    Instant lastDataAge;

    /**
     * Wall-clock time of the most recent {@code load()} that threw an {@link java.io.IOException}; {@code null} if none.
     */
    Instant lastErrorAt;

    /**
     * The most recent {@link java.io.IOException} thrown by {@code load()}; {@code null} if none.
     */
    Throwable lastError;

    /**
     * Ordered snapshot of filenames currently held in the loader's cache; empty until first successful load.
     */
    Set<String> loadedFiles = Set.of();

    public Instant getLastCheckedAt() {
        return lastCheckedAt;
    }

    public Instant getLastChangedAt() {
        return lastChangedAt;
    }

    public Instant getLastDataAge() {
        return lastDataAge;
    }

    public Instant getLastErrorAt() {
        return lastErrorAt;
    }

    public Throwable getLastError() {
        return lastError;
    }

    public Set<String> getLoadedFiles() {
        return loadedFiles;
    }

}
