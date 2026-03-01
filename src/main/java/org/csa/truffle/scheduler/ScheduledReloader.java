package org.csa.truffle.scheduler;

import org.csa.truffle.graal.GraalPyInterpreter;
import org.csa.truffle.loader.FileChangeInfo;
import org.csa.truffle.loader.FileLoader;
import org.csa.truffle.loader.FileLoaderStatus;
import org.csa.truffle.loader.LoadResult;
import org.csa.truffle.source.FileSource;
import org.csa.truffle.source.FileSourceConfig;
import org.csa.truffle.source.FileSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages a single dataset backed by a {@link FileLoader}.
 * Performs an initial synchronous reload then schedules periodic background reloads
 * at the configured interval.
 *
 * <p>A new {@link GraalPyInterpreter} is built whenever the loader detects content changes.
 * Observable status is accessible via {@link #getStatus()} and backed by {@link FileLoaderStatus}.
 *
 * <p>Thread-safety: {@code fatalError} and {@code firstErrorAt} are {@code volatile} — writes
 * from the scheduler thread are immediately visible to any reader.
 */
public class ScheduledReloader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ScheduledReloader.class);

    /**
     * Called whenever the dataset is reloaded and its content has changed (including the initial
     * load on {@code start()}), or when the grace period is exceeded (interpreter is {@code null}).
     */
    @FunctionalInterface
    public interface ReloadCallback {
        void onReload(FileLoaderStatus status, GraalPyInterpreter interpreter);
    }

    private final FileLoader loader;
    private final SchedulerConfig schedulerConfig;
    private final ReloadCallback callback;
    private ScheduledExecutorService executor;

    volatile RuntimeException fatalError;
    private volatile Instant firstErrorAt;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    public ScheduledReloader(FileSourceConfig sourceConfig, SchedulerConfig schedulerConfig,
                             ReloadCallback callback) {
        this(FileSourceFactory.create(sourceConfig), schedulerConfig, callback);
    }

    public ScheduledReloader(FileSource source, SchedulerConfig schedulerConfig,
                             ReloadCallback callback) {
        this.loader = new FileLoader(source);
        this.schedulerConfig = schedulerConfig;
        this.callback = callback;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Performs the initial reload synchronously on the calling thread
     * (so data is ready before Flink starts calling {@code processElement}),
     * then schedules periodic background reloads at the configured interval.
     */
    public void start() throws IOException {
        doReload();   // synchronous initial load

        long interval = schedulerConfig.interval().toMillis();
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ScheduledReloader");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleAtFixedRate(this::doReloadQuietly, interval, interval, TimeUnit.MILLISECONDS);

        log.info("ScheduledReloader started");
    }

    // -------------------------------------------------------------------------
    // Reload helpers
    // -------------------------------------------------------------------------

    private void doReload() throws IOException {
        LoadResult result = loader.load();

        if (!result.success()) {
            throw new IOException("FileLoader failed: " + result.error().getMessage(), result.error());
        }

        boolean needsRebuild = result.changes().stream()
                .anyMatch(c -> c.status() != FileChangeInfo.ChangeStatus.UNMODIFIED);

        if (needsRebuild) {
            try {
                GraalPyInterpreter interpreter = new GraalPyInterpreter(loader.getFileContents());
                callback.onReload(result.status(), interpreter);
            } catch (Exception e) {
                log.error("Interpreter rebuild failed: {}", e.getMessage(), e);
            }
        }

        firstErrorAt = null;  // clear error streak on success
    }

    private void doReloadQuietly() {
        try {
            doReload();
        } catch (IOException e) {
            log.error("Scheduled reload failed", e);

            Duration grace = schedulerConfig.gracePeriod();
            if (grace != null && grace.compareTo(Duration.ZERO) > 0 && fatalError == null) {
                if (firstErrorAt == null) {
                    firstErrorAt = Instant.now();
                }
                Duration streak = Duration.between(firstErrorAt, Instant.now());
                if (streak.compareTo(grace) >= 0) {
                    String msg = String.format(
                            "Python script reload grace period exceeded: errors for %ds " +
                                    "(grace: %ds). Last error: %s",
                            streak.toSeconds(), grace.toSeconds(), e.getMessage());
                    fatalError = new RuntimeException(msg, e);
                    log.error("Grace period exceeded: {}", msg);
                    callback.onReload(loader.getStatus(), null);
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Public accessors
    // -------------------------------------------------------------------------

    /**
     * Returns the {@link FileLoaderStatus} of the managed dataset.
     */
    public FileLoaderStatus getStatus() {
        return loader.getStatus();
    }

    public RuntimeException getFatalError() {
        return fatalError;
    }

    /**
     * Returns the start of the current error streak, or {@code null} if no errors have occurred.
     */
    public Instant getFirstErrorAt() {
        return firstErrorAt;
    }

    // -------------------------------------------------------------------------
    // Fatal-error checks
    // -------------------------------------------------------------------------

    /**
     * Throws the stored fatal error if the grace period has been exceeded; no-op otherwise.
     */
    public void checkForFatalError() {
        if (fatalError != null) {
            throw fatalError;
        }
    }

    // -------------------------------------------------------------------------
    // Close
    // -------------------------------------------------------------------------

    @Override
    public void close() {
        if (executor != null) {
            executor.shutdownNow();
        }
        loader.close();
    }
}
