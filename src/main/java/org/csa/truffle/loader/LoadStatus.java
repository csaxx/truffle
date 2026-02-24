package org.csa.truffle.loader;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

/**
 * Observable status for a {@link FileLoader}.
 *
 * <p>Fields are written by {@link FileLoader} (package-private access)
 * after every {@link FileLoader#load()} call.
 * All getters are public for external observation.
 */
public class LoadStatus {

    /** Wall-clock time of the most recent {@code load()} call; {@code null} until the first call. */
    volatile Instant lastCheckedAt;

    /** Wall-clock time of the most recent load that detected a change; {@code null} until then. */
    volatile Instant lastChangedAt;

    /** Max modification time across all files from the most recent {@code load()}; empty when none reported. */
    volatile Optional<Instant> lastDataAge = Optional.empty();

    /** Wall-clock time of the most recent {@code load()} that threw an {@link java.io.IOException}; {@code null} if none. */
    volatile Instant lastErrorAt;

    /** The most recent {@link java.io.IOException} thrown by {@code load()}; {@code null} if none. */
    volatile Throwable lastError;

    /** Start of the current uninterrupted error streak; cleared ({@code null}) when a load succeeds. */
    volatile Instant firstErrorAt;

    /** Ordered snapshot of filenames currently held in the loader's cache; empty until first successful load. */
    volatile Set<String> loadedFiles = Set.of();

    public Instant getLastCheckedAt()          { return lastCheckedAt; }
    public Instant getLastChangedAt()          { return lastChangedAt; }
    public Optional<Instant> getLastDataAge()  { return lastDataAge; }
    public Instant getLastErrorAt()            { return lastErrorAt; }
    public Throwable getLastError()            { return lastError; }
    public Instant getFirstErrorAt()           { return firstErrorAt; }
    public Set<String> getLoadedFiles()        { return loadedFiles; }
}
