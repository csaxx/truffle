package org.csa.truffle.graal.reload;

import org.csa.truffle.graal.GraalPyInterpreter;
import org.csa.truffle.graal.source.PythonSource;
import org.csa.truffle.graal.source.PythonSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Performs an initial synchronous reload then schedules periodic background reloads
 * of one or more {@link GraalPyInterpreter} instances at configurable intervals.
 *
 * <p>Observable status (instants, last result, error info) lives in {@link ReloaderStatus};
 * obtain it via {@link #getStatus()} (single-dataset) or {@link #getStatus(String)} (multi-dataset).
 *
 * <p>Thread-safety: all status fields are {@code volatile} — writes from the scheduler
 * thread are immediately visible to any reader.
 */
public class ScheduledReloader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ScheduledReloader.class);

    private static final String DEFAULT_ID = "default";

    /** Runtime state for one named dataset. */
    private record DatasetEntry(
            GraalPyInterpreter interpreter,
            ReloaderStatus status,
            SchedulerConfig config
    ) {}

    private final List<DatasetConfig> datasetConfigs;

    // Runtime state, populated by start().
    private final Map<String, DatasetEntry> datasets = new LinkedHashMap<>();

    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "ScheduledReloader");
                t.setDaemon(true);
                return t;
            });

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    public ScheduledReloader(List<DatasetConfig> configs) {
        this.datasetConfigs = List.copyOf(configs);
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
        for (DatasetConfig cfg : datasetConfigs) {
            PythonSource source = PythonSourceFactory.create(cfg.sourceConfig());
            GraalPyInterpreter interp = new GraalPyInterpreter(source);
            activateDataset(cfg.id(), interp, cfg.schedulerConfig());
        }
        log.info("ScheduledReloader started: {} dataset(s)", datasets.size());
    }

    private void activateDataset(String id, GraalPyInterpreter interp,
                                 SchedulerConfig config) throws IOException {
        ReloaderStatus status = new ReloaderStatus();
        DatasetEntry entry = new DatasetEntry(interp, status, config);
        datasets.put(id, entry);

        doReload(entry);   // synchronous initial reload

        long ms = config.interval().toMillis();
        executor.scheduleAtFixedRate(() -> doReloadQuietly(entry), ms, ms, TimeUnit.MILLISECONDS);
    }

    // -------------------------------------------------------------------------
    // Reload helpers
    // -------------------------------------------------------------------------

    private void doReload(DatasetEntry entry) throws IOException {
        ReloadResult result = entry.interpreter().reload();
        ReloaderStatus s = entry.status();
        s.lastResult    = result;
        s.lastCheckedAt = result.reloadedAt();
        if (result.changed()) s.lastChangedAt = result.reloadedAt();
        s.firstErrorAt = null;   // recovery: reset streak
    }

    private void doReloadQuietly(DatasetEntry entry) {
        try {
            doReload(entry);
        } catch (IOException e) {
            ReloaderStatus s = entry.status();
            Instant now = Instant.now();
            s.lastErrorAt = now;
            s.lastError   = e;
            if (s.firstErrorAt == null) s.firstErrorAt = now;   // start of streak
            log.error("Scheduled reload failed", e);

            Duration grace = entry.config().gracePeriod();
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

    public ReloaderStatus getStatus(String id) {
        return datasets.get(id).status();
    }

    public Set<String> getDatasetIds() {
        return Collections.unmodifiableSet(datasets.keySet());
    }

    // -------------------------------------------------------------------------
    // Public accessors — backward-compat single-dataset
    // -------------------------------------------------------------------------

    /** Returns the status of the default (single) dataset. */
    public ReloaderStatus getStatus() {
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
