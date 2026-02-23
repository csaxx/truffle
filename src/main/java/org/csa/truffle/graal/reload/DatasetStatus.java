package org.csa.truffle.graal.reload;

import java.time.Instant;

/**
 * Holds the observable status state for a {@link ScheduledReloader}.
 *
 * <p>All fields are package-private so {@code ScheduledReloader} can write them directly.
 * All accessor methods are public for external observation.
 */
public class DatasetStatus {

    volatile Instant lastCheckedAt;            // null until first check
    volatile Instant lastChangedAt;            // null until first change
    volatile Instant lastErrorAt;              // null if no error yet
    volatile Throwable lastError;              // null if no error yet
    volatile ReloadResult lastResult;          // null until first reload
    volatile Instant firstErrorAt;             // start of current error streak; cleared on success
    volatile RuntimeException fatalError;      // non-null once grace period is exceeded

    public Instant getLastCheckedAt() {
        return lastCheckedAt;
    }

    public Instant getLastChangedAt() {
        return lastChangedAt;
    }

    public Instant getLastErrorAt() {
        return lastErrorAt;
    }

    public Throwable getLastError() {
        return lastError;
    }

    public ReloadResult getLastResult() {
        return lastResult;
    }

    /** Start of the current error streak, or {@code null} if no errors or last reload succeeded. */
    public Instant getFirstErrorAt() {
        return firstErrorAt;
    }

    /**
     * Throws the stored fatal error if the grace period has been exceeded; no-op otherwise.
     */
    public void checkForFatalError() {
        RuntimeException e = fatalError;
        if (e != null) throw e;
    }
}
