package org.csa.truffle.graal.reload;

import org.csa.truffle.graal.GraalPyInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Performs an initial synchronous reload then schedules periodic background reloads
 * of a {@link GraalPyInterpreter} at a configurable interval.
 *
 * <p>Observable status (instants, last result, error info) lives in {@link ReloaderStatus};
 * obtain it via {@link #getStatus()}.
 *
 * <p>Thread-safety: all status fields are {@code volatile} — writes from the scheduler
 * thread are immediately visible to any reader.
 */
public class ScheduledReloader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ScheduledReloader.class);

    private final GraalPyInterpreter interpreter;
    private final SchedulerConfig config;
    private final ScheduledExecutorService executor;
    private final ReloaderStatus status = new ReloaderStatus();

    public ScheduledReloader(GraalPyInterpreter interpreter, SchedulerConfig config) {
        this.interpreter = interpreter;
        this.config = config;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ScheduledReloader");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Performs the initial reload synchronously on the calling thread
     * (so data is ready before Flink starts calling {@code processElement}),
     * then schedules periodic background reloads at the configured interval.
     */
    public void start() throws IOException {
        doReload();   // synchronous, on open() thread
        long ms = config.interval().toMillis();
        executor.scheduleAtFixedRate(this::doReloadQuietly, ms, ms, TimeUnit.MILLISECONDS);
        log.info("ScheduledReloader started: interval={}", config.interval());
    }

    private void doReload() throws IOException {
        ReloadResult result = interpreter.reload();
        status.lastResult    = result;
        status.lastCheckedAt = result.reloadedAt();
        if (result.changed()) status.lastChangedAt = result.reloadedAt();
        status.firstErrorAt = null;   // recovery: reset streak
    }

    private void doReloadQuietly() {
        try {
            doReload();
        } catch (IOException e) {
            Instant now = Instant.now();
            status.lastErrorAt = now;
            status.lastError   = e;
            if (status.firstErrorAt == null) status.firstErrorAt = now;   // start of streak
            log.error("Scheduled reload failed", e);

            Duration grace = config.gracePeriod();
            if (grace != null && grace.compareTo(Duration.ZERO) > 0) {
                Duration streakDuration = Duration.between(status.firstErrorAt, now);
                if (status.fatalError == null && streakDuration.compareTo(grace) >= 0) {
                    String msg = String.format(
                            "Python script reload grace period exceeded: errors for %ds " +
                            "(grace: %ds). Last error: %s",
                            streakDuration.toSeconds(), grace.toSeconds(), e.getMessage());
                    status.fatalError = new RuntimeException(msg, e);
                    log.error("Grace period exceeded — Flink task will be failed: {}", msg);
                }
            }
        }
    }

    /** Returns the observable status of this reloader. */
    public ReloaderStatus getStatus() {
        return status;
    }

    /**
     * Throws the stored fatal error if the grace period has been exceeded; no-op otherwise.
     * Delegates to {@link ReloaderStatus#checkForFatalError()}.
     */
    public void checkForFatalError() {
        status.checkForFatalError();
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
