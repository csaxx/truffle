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
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
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

    private static final String DEFAULT_ID = "default";

    /**
     * Identifies one named dataset: its source, and the schedule controlling how often it reloads.
     *
     * <p>{@code id} goes here rather than into {@link SchedulerConfig} — scheduling behaviour
     * (interval/grace period) is separate from dataset identity, and the same
     * {@code SchedulerConfig} can be reused across multiple datasets.
     */
    record DatasetRegistration(
            String id,
            FileSourceConfig sourceConfig,   // null when directSource is set
            FileSource directSource,          // null when sourceConfig is set
            SchedulerConfig schedulerConfig,
            DatasetReloadCallback callback
    ) implements Serializable {}

    /**
     * Called by {@link ScheduledReloader} whenever a dataset is reloaded and its
     * content has changed (including the initial load on {@code start()}).
     */
    @FunctionalInterface
    public interface DatasetReloadCallback {
        void onReload(String datasetId, LoadStatus status, GraalPyInterpreter interpreter);
    }

    /** Mutable runtime state for one named dataset. */
    private static final class DatasetState {
        final String id;
        final SchedulerConfig schedulerConfig;
        final DatasetReloadCallback callback;
        final FileLoader loader;
        volatile GraalPyInterpreter interpreter;      // rebuilt when files change
        volatile RuntimeException fatalError;         // set once grace period exceeded
        volatile Instant lastBuiltChangedAt;          // lastChangedAt when interpreter was last built

        DatasetState(String id, SchedulerConfig schedulerConfig,
                     DatasetReloadCallback callback, FileLoader loader) {
            this.id = id;
            this.schedulerConfig = schedulerConfig;
            this.callback = callback;
            this.loader = loader;
        }
    }

    private final List<DatasetRegistration> registrations = new ArrayList<>();

    // Runtime states, populated by start().
    private final Map<String, DatasetState> datasets = new LinkedHashMap<>();

    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "ScheduledReloader");
                t.setDaemon(true);
                return t;
            });

    // -------------------------------------------------------------------------
    // Registration
    // -------------------------------------------------------------------------

    /**
     * Registers a dataset to be loaded and periodically reloaded.
     * Must be called before {@link #start()}.
     */
    public void register(String id,
                         FileSourceConfig sourceConfig,
                         SchedulerConfig schedulerConfig,
                         DatasetReloadCallback callback) {
        registrations.add(new DatasetRegistration(id, sourceConfig, null, schedulerConfig, callback));
    }

    /**
     * Registers a dataset backed by a pre-built {@link FileSource} instance.
     * Use this when the source is not serializable (e.g. a test helper).
     */
    public void register(String id,
                         FileSource source,
                         SchedulerConfig schedulerConfig,
                         DatasetReloadCallback callback) {
        registrations.add(new DatasetRegistration(id, null, source, schedulerConfig, callback));
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
        for (DatasetRegistration registration : registrations) {
            FileSource source = registration.directSource() != null
                    ? registration.directSource()
                    : FileSourceFactory.create(registration.sourceConfig());
            FileLoader loader = new FileLoader(source);
            activateDataset(registration, loader);
        }
        log.info("ScheduledReloader started: {} dataset(s)", datasets.size());
    }

    private void activateDataset(DatasetRegistration registration, FileLoader loader) throws IOException {
        DatasetState entry = new DatasetState(
                registration.id(), registration.schedulerConfig(), registration.callback(), loader);
        datasets.put(registration.id(), entry);
        doReload(entry);   // synchronous initial load
        long interval = registration.schedulerConfig().interval().toMillis();
        executor.scheduleAtFixedRate(() -> doReloadQuietly(entry), interval, interval, TimeUnit.MILLISECONDS);
    }

    // -------------------------------------------------------------------------
    // Reload helpers
    // -------------------------------------------------------------------------

    private void doReload(DatasetState entry) throws IOException {
        LoadResult result = entry.loader.load();

        if (!result.success()) {
            throw new IOException("FileLoader failed: " + result.error().getMessage(), result.error());
        }

        LoadStatus status = result.status();
        Instant newChangedAt = status.getLastChangedAt();
        boolean needsRebuild = (entry.interpreter == null)
                || (newChangedAt != null && !newChangedAt.equals(entry.lastBuiltChangedAt));

        if (needsRebuild) {
            GraalPyInterpreter old = entry.interpreter;
            try {
                entry.interpreter = new GraalPyInterpreter(result.fileContents());
                entry.lastBuiltChangedAt = newChangedAt;
                if (old != null) old.close();
                entry.callback.onReload(entry.id, status, entry.interpreter);
            } catch (Exception e) {
                log.error("Interpreter rebuild failed for dataset '{}': {}", entry.id, e.getMessage(), e);
                if (entry.interpreter == null) {
                    // Initial load — nothing to fall back to
                    throw new IOException("Initial interpreter build failed for '" + entry.id + "'", e);
                }
                // Subsequent rebuild failure — keep old interpreter running; don't track streak
            }
        }
    }

    private void doReloadQuietly(DatasetState entry) {
        try {
            doReload(entry);
        } catch (IOException e) {
            LoadStatus status = entry.loader.getStatus();
            log.error("Scheduled reload failed for dataset '{}'", entry.id, e);

            Duration grace = entry.schedulerConfig.gracePeriod();
            if (grace != null && grace.compareTo(Duration.ZERO) > 0 && entry.fatalError == null) {
                Instant streakStart = status.getFirstErrorAt();
                if (streakStart != null) {
                    Duration streak = Duration.between(streakStart, Instant.now());
                    if (streak.compareTo(grace) >= 0) {
                        String msg = String.format(
                                "Python script reload grace period exceeded: errors for %ds " +
                                "(grace: %ds). Last error: %s",
                                streak.toSeconds(), grace.toSeconds(), e.getMessage());
                        entry.fatalError = new RuntimeException(msg, e);
                        log.error("Grace period exceeded for dataset '{}': {}", entry.id, msg);
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Public accessors — multi-dataset
    // -------------------------------------------------------------------------

    public GraalPyInterpreter getInterpreter(String id) { return datasets.get(id).interpreter; }
    public LoadStatus getStatus(String id)               { return datasets.get(id).loader.getStatus(); }
    public Set<String> getDatasetIds()                   { return Collections.unmodifiableSet(datasets.keySet()); }

    // -------------------------------------------------------------------------
    // Public accessors — backward-compat single-dataset
    // -------------------------------------------------------------------------

    /** Returns the status of the default (single) dataset. */
    public LoadStatus getStatus() {
        return getStatus(DEFAULT_ID);
    }

    // -------------------------------------------------------------------------
    // Fatal-error checks
    // -------------------------------------------------------------------------

    /**
     * Throws the stored fatal error for the named dataset if the grace period has been exceeded;
     * no-op otherwise.
     */
    public void checkForFatalErrorById(String id) {
        RuntimeException e = datasets.get(id).fatalError;
        if (e != null) throw e;
    }

    /**
     * Throws the stored fatal error if the grace period has been exceeded for <em>any</em> dataset;
     * no-op otherwise.
     */
    public void checkForFatalError() {
        for (DatasetState entry : datasets.values()) {
            RuntimeException e = entry.fatalError;
            if (e != null) throw e;
        }
    }

    // -------------------------------------------------------------------------
    // Close
    // -------------------------------------------------------------------------

    @Override
    public void close() {
        executor.shutdownNow();
        for (DatasetState entry : datasets.values()) {
            if (entry.interpreter != null) entry.interpreter.close();
            try { entry.loader.close(); } catch (IOException ignored) {}
        }
    }
}
