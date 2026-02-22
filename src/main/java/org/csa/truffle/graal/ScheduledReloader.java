package org.csa.truffle.graal;

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
 * <p>Thread-safety:
 * <ul>
 *   <li>{@link GraalPyInterpreter#reload()} is {@code synchronized} — safe from any thread.</li>
 *   <li>{@code lastCheckedAt} / {@code lastChangedAt} are {@code volatile Instant} (Instant is
 *       immutable) — writes from the scheduler thread are immediately visible to any reader.</li>
 * </ul>
 */
public class ScheduledReloader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ScheduledReloader.class);

    private final GraalPyInterpreter interpreter;
    private final Duration interval;
    private final ScheduledExecutorService executor;

    private volatile Instant lastCheckedAt;   // null until first check
    private volatile Instant lastChangedAt;   // null until first change

    public ScheduledReloader(GraalPyInterpreter interpreter, Duration interval) {
        this.interpreter = interpreter;
        this.interval = interval;
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
        long ms = interval.toMillis();
        executor.scheduleAtFixedRate(this::doReloadQuietly, ms, ms, TimeUnit.MILLISECONDS);
        log.info("ScheduledReloader started: interval={}", interval);
    }

    private void doReload() throws IOException {
        boolean changed = interpreter.reload();
        Instant now = Instant.now();
        lastCheckedAt = now;
        if (changed) {
            lastChangedAt = now;
        }
    }

    private void doReloadQuietly() {
        try {
            doReload();
        } catch (IOException e) {
            log.error("Scheduled reload failed", e);
        }
    }

    public Instant getLastCheckedAt() { return lastCheckedAt; }
    public Instant getLastChangedAt() { return lastChangedAt; }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
