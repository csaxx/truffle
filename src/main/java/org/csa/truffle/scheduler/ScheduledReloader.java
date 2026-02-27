package org.csa.truffle.scheduler;

import org.csa.truffle.graal.GraalPyInterpreter;
import org.csa.truffle.loader.FileLoader;
import org.csa.truffle.loader.LoadResult;
import org.csa.truffle.loader.LoadStatus;
import org.csa.truffle.loader.source.FileSource;
import org.csa.truffle.loader.source.FileSourceConfig;
import org.csa.truffle.loader.source.FileSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Performs an initial synchronous reload then schedules periodic background reloads
 * of one or more {@link GraalPyInterpreter} instances at configurable intervals.
 *
 * <p>File loading and change detection are delegated to {@link FileLoader}.
 * A new {@link GraalPyInterpreter} is built whenever the loader detects content changes.
 * Observable status (instants, error info) is accessible via {@link #getStatus()} or
 * {@link #getStatus(String)} and backed by {@link LoadStatus}.
 *
 * <p>Thread-safety: interpreter and fatalError fields are {@code volatile} — writes
 * from the scheduler thread are immediately visible to any reader.
 */
public class ScheduledReloader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ScheduledReloader.class);

    /**
     * Called by {@link ScheduledReloader} whenever a dataset is reloaded and its
     * content has changed (including the initial load on {@code start()}).
     */
    @FunctionalInterface
    public interface DatasetReloadCallback {
        void onReload(LoadStatus status, GraalPyInterpreter interpreter);
    }


    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "ScheduledReloader");
                t.setDaemon(true);
                return t;
            });

    private final FileSourceConfig sourceConfig; // null when directSource is set
    private final SchedulerConfig schedulerConfig;
    private final DatasetReloadCallback callback;

    private FileLoader loader;
    volatile RuntimeException fatalError;         // set once grace period exceeded
    volatile Instant lastBuiltChangedAt;          // lastChangedAt when interpreter was last built

    public ScheduledReloader(FileSourceConfig sourceConfig, SchedulerConfig schedulerConfig, DatasetReloadCallback callback) {
        this.sourceConfig = sourceConfig;
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
        FileSource source = FileSourceFactory.create(sourceConfig);
        FileLoader loader = new FileLoader(source);

        doReload();   // synchronous initial load
        long interval = schedulerConfig.interval().toMillis();
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

        LoadStatus status = result.status();
        Instant newChangedAt = status.getLastChangedAt();
        boolean needsRebuild = (newChangedAt != null && !newChangedAt.equals(lastBuiltChangedAt));

        if (needsRebuild) {
            GraalPyInterpreter old = entry.interpreter;
            try {
                GraalPyInterpreter interpreter = new GraalPyInterpreter(result.fileContents());
                lastBuiltChangedAt = newChangedAt;
                if (old != null) old.close();
                callback.onReload(entry.id, status, entry.interpreter);
            } catch (Exception e) {
                log.error("Interpreter rebuild failed for dataset '{}': {}", entry.id, e.getMessage(), e);
            }
        }
    }

    private void doReloadQuietly() {
        try {
            doReload();
        } catch (IOException e) {
            LoadStatus status = loader.getStatus();
            log.error("Scheduled reload failed for dataset '{}'", entry.id, e);

            Duration grace = schedulerConfig.gracePeriod();
            if (grace != null && grace.compareTo(Duration.ZERO) > 0 && fatalError == null) {
                Instant streakStart = status.getFirstErrorAt();
                if (streakStart != null) {
                    Duration streak = Duration.between(streakStart, Instant.now());
                    if (streak.compareTo(grace) >= 0) {
                        String msg = String.format(
                                "Python script reload grace period exceeded: errors for %ds " +
                                        "(grace: %ds). Last error: %s",
                                streak.toSeconds(), grace.toSeconds(), e.getMessage());
                        fatalError = new RuntimeException(msg, e);
                        log.error("Grace period exceeded for dataset '{}': {}", entry.id, msg);
                    }
                }
            }
        }
    }


    // -------------------------------------------------------------------------
    // Public accessors — backward-compat single-dataset
    // -------------------------------------------------------------------------

    /**
     * Returns the status of the default (single) dataset.
     */
    public LoadStatus getStatus() {
        return loader != null ? loader.getStatus() : null;
    }

    public RuntimeException getFatalError() {
        return fatalError;
    }

    // -------------------------------------------------------------------------
    // Fatal-error checks
    // -------------------------------------------------------------------------

    /**
     * Throws the stored fatal error if the grace period has been exceeded for <em>any</em> dataset;
     * no-op otherwise.
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
        executor.shutdownNow();

        try {
            loader.close();
        } catch (IOException ignored) {
        }
    }

}
