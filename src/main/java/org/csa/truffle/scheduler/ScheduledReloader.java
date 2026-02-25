package org.csa.truffle.scheduler;

import org.csa.truffle.graal.GraalPyInterpreter;
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
        void onReload(String datasetId, DatasetStatus status, GraalPyInterpreter interpreter);
    }

    /** Runtime state for one named dataset. */
    private record DatasetState(
            String id,
            FileSourceConfig sourceConfig,
            SchedulerConfig schedulerConfig,
            GraalPyInterpreter interpreter,
            DatasetStatus status,
            DatasetReloadCallback callback
    ) {}

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
            GraalPyInterpreter interpreter = new GraalPyInterpreter(source);
            activateDataset(registration, interpreter);
        }

        log.info("ScheduledReloader started: {} dataset(s)", datasets.size());
    }

    private void activateDataset(DatasetRegistration registration, GraalPyInterpreter interpreter) throws IOException {

        DatasetStatus status = new DatasetStatus();
        DatasetState entry = new DatasetState(registration.id(), registration.sourceConfig(),
                registration.schedulerConfig(), interpreter, status, registration.callback);
        datasets.put(registration.id(), entry);

        doReload(entry);   // synchronous initial reload — always fires callback

        long interval = registration.schedulerConfig().interval().toMillis();
        executor.scheduleAtFixedRate(() -> doReloadQuietly(entry), interval, interval, TimeUnit.MILLISECONDS);
    }

    // -------------------------------------------------------------------------
    // Reload helpers
    // -------------------------------------------------------------------------

    private void doReload(DatasetState entry) throws IOException {

        Instant reloadedAt = Instant.now();
        boolean changed = entry.interpreter().reload();
        DatasetStatus status = entry.status();
        status.lastCheckedAt = reloadedAt;

        if (changed) {
            status.lastChangedAt = reloadedAt;
            status.lastDataAge   = entry.interpreter().getDataAge();
            entry.callback().onReload(entry.id(), status, entry.interpreter());
        }

        status.firstErrorAt = null;   // recovery: reset streak
    }

    private void doReloadQuietly(DatasetState entry) {
        try {
            doReload(entry);
        } catch (IOException e) {
            DatasetStatus s = entry.status();
            Instant now = Instant.now();
            s.lastErrorAt = now;
            s.lastError   = e;
            if (s.firstErrorAt == null) s.firstErrorAt = now;   // start of streak
            log.error("Scheduled reload failed", e);

            Duration grace = entry.schedulerConfig().gracePeriod();

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
