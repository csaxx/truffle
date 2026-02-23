package org.csa.truffle.graal.reload;

import org.csa.truffle.graal.GraalPyInterpreter;
import org.csa.truffle.graal.source.PythonSource;
import org.csa.truffle.graal.source.PythonSourceConfig;
import org.csa.truffle.graal.source.PythonSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Performs an initial synchronous reload then schedules periodic background reloads
 * of one or more {@link GraalPyInterpreter} instances at configurable intervals.
 *
 * <p>Observable status (instants, last result, error info) lives in {@link DatasetStatus};
 * obtain it via {@link #getStatus()} (single-dataset) or {@link #getStatus(String)} (multi-dataset).
 *
 * <p>Thread-safety: all status fields are {@code volatile} — writes from the scheduler
 * thread are immediately visible to any reader.
 */
public class ScheduledReloader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ScheduledReloader.class);

    private static final String DEFAULT_ID = "default";

    /**
     * Called by {@link ScheduledReloader} whenever a dataset is reloaded and its
     * content has changed (including the initial load on {@code start()}).
     */
    @FunctionalInterface
    public interface DatasetReloadCallback {
        void onReload(String datasetId, DatasetStatus status, GraalPyInterpreter interpreter);
    }

    private record RegistrationEntry(DatasetConfig config, DatasetReloadCallback callback) {}

    /** Runtime state for one named dataset. */
    private record DatasetEntry(
            DatasetConfig datasetConfig,
            GraalPyInterpreter interpreter,
            DatasetStatus status,
            DatasetReloadCallback callback
    ) {}

    private final Map<String, RegistrationEntry> registrations = new LinkedHashMap<>();

    // Runtime state, populated by start().
    private final Map<String, DatasetEntry> datasets = new LinkedHashMap<>();

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
                         PythonSourceConfig sourceConfig,
                         SchedulerConfig schedulerConfig,
                         DatasetReloadCallback callback) {
        registrations.put(id, new RegistrationEntry(
                new DatasetConfig(id, sourceConfig, schedulerConfig), callback));
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
        for (RegistrationEntry reg : registrations.values()) {
            PythonSource source = PythonSourceFactory.create(reg.config().sourceConfig());
            GraalPyInterpreter interp = new GraalPyInterpreter(source);
            activateDataset(reg.config(), interp, reg.callback());
        }
        log.info("ScheduledReloader started: {} dataset(s)", datasets.size());
    }

    private void activateDataset(DatasetConfig config, GraalPyInterpreter interp,
                                 DatasetReloadCallback callback) throws IOException {
        DatasetStatus status = new DatasetStatus();
        DatasetEntry entry = new DatasetEntry(config, interp, status, callback);
        datasets.put(config.id(), entry);

        doReload(entry);   // synchronous initial reload — always fires callback

        long ms = config.schedulerConfig().interval().toMillis();
        executor.scheduleAtFixedRate(() -> doReloadQuietly(entry), ms, ms, TimeUnit.MILLISECONDS);
    }

    // -------------------------------------------------------------------------
    // Reload helpers
    // -------------------------------------------------------------------------

    private void doReload(DatasetEntry entry) throws IOException {
        ReloadResult result = entry.interpreter().reload();
        DatasetStatus status = entry.status();
        status.lastResult    = result;
        status.lastCheckedAt = result.reloadedAt();
        if (result.changed()) {
            status.lastChangedAt = result.reloadedAt();
            entry.callback().onReload(entry.datasetConfig().id(), status, entry.interpreter());
        }
        status.firstErrorAt = null;   // recovery: reset streak
    }

    private void doReloadQuietly(DatasetEntry entry) {
        try {
            doReload(entry);
        } catch (IOException e) {
            DatasetStatus s = entry.status();
            Instant now = Instant.now();
            s.lastErrorAt = now;
            s.lastError   = e;
            if (s.firstErrorAt == null) s.firstErrorAt = now;   // start of streak
            log.error("Scheduled reload failed", e);

            Duration grace = entry.datasetConfig().schedulerConfig().gracePeriod();
            if (grace != null && grace.compareTo(Duration.ZERO) > 0) {
                Duration streakDuration = Duration.between(s.firstErrorAt, now);
                if (s.fatalError == null && streakDuration.compareTo(grace) >= 0) {
                    String msg = String.format(
                            "Python script reload grace period exceeded: errors for %ds " +
                                    "(grace: %ds). Last error: %s",
                            streakDuration.toSeconds(), grace.toSeconds(), e.getMessage());
                    s.fatalError = new RuntimeException(msg, e);
                    log.error("Grace period exceeded — Flink task will be failed: {}", msg);
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Public accessors — multi-dataset
    // -------------------------------------------------------------------------

    public GraalPyInterpreter getInterpreter(String id) {
        return datasets.get(id).interpreter();
    }

    public DatasetStatus getStatus(String id) {
        return datasets.get(id).status();
    }

    public Set<String> getDatasetIds() {
        return Collections.unmodifiableSet(datasets.keySet());
    }

    // -------------------------------------------------------------------------
    // Public accessors — backward-compat single-dataset
    // -------------------------------------------------------------------------

    /** Returns the status of the default (single) dataset. */
    public DatasetStatus getStatus() {
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
        getStatus(id).checkForFatalError();
    }

    /**
     * Throws the stored fatal error if the grace period has been exceeded for <em>any</em> dataset;
     * no-op otherwise.
     */
    public void checkForFatalError() {
        datasets.values().forEach(e -> e.status().checkForFatalError());
    }

    // -------------------------------------------------------------------------
    // Close
    // -------------------------------------------------------------------------

    @Override
    public void close() {
        executor.shutdownNow();
        datasets.values().forEach(e -> e.interpreter().close());
    }

}
