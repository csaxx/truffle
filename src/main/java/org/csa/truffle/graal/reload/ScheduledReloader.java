package org.csa.truffle.graal.reload;

import org.csa.truffle.graal.GraalPyInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Performs an initial synchronous reload then schedules periodic background reloads
 * of a {@link GraalPyInterpreter} at a configurable interval.
 *
 * <p>Thread-safety:
 * <ul>
 *   <li>{@link GraalPyInterpreter#reload()} is {@code synchronized} — safe from any thread.</li>
 *   <li>{@code lastCheckedAt} / {@code lastChangedAt} / {@code lastResult} / {@code lastErrorAt} /
 *       {@code lastError} are {@code volatile} — writes from the scheduler thread are immediately
 *       visible to any reader.</li>
 * </ul>
 */
public class ScheduledReloader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ScheduledReloader.class);

    private final GraalPyInterpreter interpreter;
    private final SchedulerConfig config;
    private final ScheduledExecutorService executor;

    private volatile Instant lastCheckedAt;   // null until first check
    private volatile Instant lastChangedAt;   // null until first change
    private volatile Instant lastErrorAt;     // null if no error yet
    private volatile Throwable lastError;     // null if no error yet
    private volatile ReloadResult lastResult; // null until first reload

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
        lastResult    = result;
        lastCheckedAt = result.reloadedAt();
        if (result.changed()) lastChangedAt = result.reloadedAt();
    }

    private void doReloadQuietly() {
        try {
            doReload();
        } catch (IOException e) {
            lastErrorAt = Instant.now();
            lastError   = e;
            log.error("Scheduled reload failed", e);
        }
    }

    public Instant getLastCheckedAt() {
        return lastCheckedAt;
    }

    public Instant getLastChangedAt() {
        return lastChangedAt;
    }

    public ReloadResult getLastResult() {
        return lastResult;
    }

    public Instant getLastErrorAt() {
        return lastErrorAt;
    }

    public Throwable getLastError() {
        return lastError;
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
