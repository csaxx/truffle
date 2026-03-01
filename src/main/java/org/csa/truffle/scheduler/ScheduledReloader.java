package org.csa.truffle.scheduler;

import org.csa.truffle.graal.GraalPyInterpreter;
import org.csa.truffle.loader.*;
import org.csa.truffle.source.FileSource;
import org.csa.truffle.source.FileSourceConfig;
import org.csa.truffle.source.FileSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages one or more named datasets, each backed by a {@link FileLoader}.
 * Performs an initial synchronous reload then schedules periodic background reloads
 * at the configured interval per dataset.
 *
 * <p>A new {@link GraalPyInterpreter} is built whenever the loader detects content changes.
 * Observable status is accessible via {@link #getStatus()} and backed by {@link FileLoaderStatus}.
 *
 * <p>Thread-safety: {@code fatalError} is {@code volatile} — writes from the scheduler
 * thread are immediately visible to any reader.
 */
public class ScheduledReloader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ScheduledReloader.class);

    /**
     * Called by {@link ScheduledReloader} whenever a dataset is reloaded and its
     * content has changed (including the initial load on {@code start()}).
     */
    @FunctionalInterface
    public interface DatasetReloadCallback {
        void onReload(String id, FileLoaderStatus status, GraalPyInterpreter interpreter);
    }

    /** Internal record for a registered dataset. */
    private static final class DatasetEntry {
        final String id;
        final FileLoader loader;
        final SchedulerConfig schedulerConfig;
        final DatasetReloadCallback callback;
        ScheduledExecutorService executor;

        DatasetEntry(String id, FileLoader loader, SchedulerConfig schedulerConfig, DatasetReloadCallback callback) {
            this.id = id;
            this.loader = loader;
            this.schedulerConfig = schedulerConfig;
            this.callback = callback;
        }
    }

    private final List<DatasetEntry> datasets = new ArrayList<>();

    volatile RuntimeException fatalError;

    // -------------------------------------------------------------------------
    // Construction and registration
    // -------------------------------------------------------------------------

    public ScheduledReloader() {}

    /**
     * Registers a dataset backed by the given {@link FileSourceConfig}.
     * Must be called before {@link #start()}.
     */
    public void register(String id, FileSourceConfig config, SchedulerConfig schedulerConfig,
                         DatasetReloadCallback callback) {
        FileSource source = FileSourceFactory.create(config);
        registerSource(id, source, schedulerConfig, callback);
    }

    /**
     * Registers a dataset backed by an already-constructed {@link FileSource}.
     * Must be called before {@link #start()}.
     */
    public void register(String id, FileSource source, SchedulerConfig schedulerConfig,
                         DatasetReloadCallback callback) {
        registerSource(id, source, schedulerConfig, callback);
    }

    private void registerSource(String id, FileSource source, SchedulerConfig schedulerConfig,
                                 DatasetReloadCallback callback) {
        datasets.add(new DatasetEntry(id, new FileLoader(source), schedulerConfig, callback));
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Performs the initial reload synchronously on the calling thread
     * (so data is ready before Flink starts calling {@code processElement}),
     * then schedules periodic background reloads at the configured interval per dataset.
     */
    public void start() throws IOException {
        for (DatasetEntry entry : datasets) {
            doReload(entry);   // synchronous initial load

            long interval = entry.schedulerConfig.interval().toMillis();
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "ScheduledReloader-" + entry.id);
                t.setDaemon(true);
                return t;
            });
            entry.executor = executor;
            executor.scheduleAtFixedRate(() -> doReloadQuietly(entry), interval, interval, TimeUnit.MILLISECONDS);
        }

        log.info("ScheduledReloader started ({} dataset(s))", datasets.size());
    }

    // -------------------------------------------------------------------------
    // Reload helpers
    // -------------------------------------------------------------------------

    private void doReload(DatasetEntry entry) throws IOException {
        LoadResult result = entry.loader.load();

        if (!result.success()) {
            throw new IOException("FileLoader failed: " + result.error().getMessage(), result.error());
        }

        boolean needsRebuild = result.changes().stream()
                .anyMatch(c -> c.status() != FileChangeInfo.ChangeStatus.UNMODIFIED);

        if (needsRebuild) {
            Map<String, String> contents = entry.loader.getFileContents();
            try {
                GraalPyInterpreter interpreter = new GraalPyInterpreter(contents);
                entry.callback.onReload(entry.id, result.status(), interpreter);
            } catch (Exception e) {
                log.error("Interpreter rebuild failed for dataset '{}': {}", entry.id, e.getMessage(), e);
            }
        }
    }

    private void doReloadQuietly(DatasetEntry entry) {
        try {
            doReload(entry);
        } catch (IOException e) {
            FileLoaderStatus status = entry.loader.getStatus();
            log.error("Scheduled reload failed for dataset '{}'", entry.id, e);

            Duration grace = entry.schedulerConfig.gracePeriod();
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
    // Public accessors
    // -------------------------------------------------------------------------

    /**
     * Returns the {@link FileLoaderStatus} of the first registered dataset, or {@code null}
     * if no datasets have been registered.
     */
    public FileLoaderStatus getStatus() {
        return datasets.isEmpty() ? null : datasets.get(0).loader.getStatus();
    }

    public RuntimeException getFatalError() {
        return fatalError;
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
        for (DatasetEntry entry : datasets) {
            if (entry.executor != null) {
                entry.executor.shutdownNow();
            }
            try {
                entry.loader.close();
            } catch (IOException ignored) {
            }
        }
    }
}
