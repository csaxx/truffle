package org.csa.truffle.loader;

import org.csa.truffle.loader.source.FileSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.*;

/**
 * Loads and caches the contents of Python files from a {@link FileSource},
 * using per-file modification timestamps to avoid re-reading unchanged files.
 *
 * <p>Call {@link #load()} to (re)load from the source. On the first call every
 * listed file is read. On subsequent calls a file's content is re-read only
 * when its modification time has advanced since the last load; files for which
 * the source cannot provide a timestamp are always re-read. Files that
 * disappear from {@code index.txt} are evicted from the cache.
 *
 * <p>An optional {@link Runnable} callback supplied at construction time is
 * invoked by {@link #load()} whenever the loaded set changes (additions,
 * removals, or content updates).
 *
 * <p>Operational state is tracked in a {@link LoadStatus} instance;
 * obtain it via {@link #getStatus()}.
 *
 * <p>All mutating operations are {@code synchronized}; the status fields are
 * {@code volatile} and safe to read from any thread without locking.
 */
public class FileLoader implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(FileLoader.class);

    private final FileSource source;
    private final Runnable onChanged; // nullable

    /** Ordered cache: filename → current content. Preserved across calls. */
    private LinkedHashMap<String, String> fileContents = new LinkedHashMap<>();

    /** Per-file last-known mtime. Absent when a file was loaded without a timestamp. */
    private final Map<String, Instant> lastModTimes = new HashMap<>();

    private final LoadStatus status = new LoadStatus();

    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    /** Creates a loader without a change callback. */
    public FileLoader(FileSource source) {
        this(source, null);
    }

    /**
     * Creates a loader with an optional change callback.
     *
     * @param source     the source to load Python files from
     * @param onChanged  called by {@link #load()} whenever a content change is
     *                   detected; {@code null} to disable
     */
    public FileLoader(FileSource source, Runnable onChanged) {
        this.source = source;
        this.onChanged = onChanged;
        source.setChangeListener(this::doReloadOnChange);
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Loads or refreshes file contents from the source.
     *
     * <p>For each file returned by {@link FileSource#listFiles()}:
     * <ul>
     *   <li>If a modification time is present and has not advanced since the
     *       last load, the cached content is reused without an I/O call.</li>
     *   <li>Otherwise the file is re-read via {@link FileSource#readFile}.</li>
     * </ul>
     * Files no longer listed in {@code index.txt} are evicted from the cache.
     *
     * <p>{@link LoadStatus} is updated on every call regardless of
     * whether a change occurred.
     *
     * @return {@code true} if any file was added, removed, or had updated content
     * @throws IOException if the source raises an I/O error (status is updated
     *                     with error info before the exception propagates)
     */
    public synchronized boolean load() throws IOException {
        Instant checkedAt = Instant.now();
        try {
            LinkedHashMap<String, Optional<Instant>> fileList = source.listFiles();
            log.debug("load() started; source lists {} file(s)", fileList.size());

            boolean changed = false;
            LinkedHashMap<String, String> newContents = new LinkedHashMap<>();
            Map<String, Instant> newModTimes = new HashMap<>();

            for (Map.Entry<String, Optional<Instant>> entry : fileList.entrySet()) {
                String name = entry.getKey();
                Optional<Instant> modTime = entry.getValue();

                boolean needsRead;
                if (modTime.isPresent()) {
                    Instant lastKnown = lastModTimes.get(name);
                    needsRead = lastKnown == null || modTime.get().isAfter(lastKnown);
                    newModTimes.put(name, modTime.get());
                } else {
                    needsRead = true; // no timestamp available — always re-read
                }

                String content;
                if (needsRead) {
                    content = source.readFile(name);
                    String previous = fileContents.get(name);
                    if (!content.equals(previous)) {
                        log.info(previous == null ? "New file loaded: {}" : "File content updated: {}", name);
                        changed = true;
                    }
                } else {
                    content = fileContents.get(name); // reuse from cache
                    log.debug("Skipped unchanged file (modTime): {}", name);
                }
                newContents.put(name, content);
            }

            // Detect removed files
            for (String name : fileContents.keySet()) {
                if (!newContents.containsKey(name)) {
                    log.info("File removed from index: {}", name);
                    changed = true;
                }
            }

            fileContents = newContents;
            lastModTimes.clear();
            lastModTimes.putAll(newModTimes);

            Optional<Instant> maxDataAge = fileList.values().stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .max(Comparator.naturalOrder());

            status.lastCheckedAt = checkedAt;
            status.lastDataAge = maxDataAge;
            status.loadedFiles = Set.copyOf(newContents.keySet());
            status.firstErrorAt = null; // clear error streak on success

            if (changed) {
                status.lastChangedAt = checkedAt;
                log.debug("load() complete: change(s) detected");
                if (onChanged != null) onChanged.run();
            } else {
                log.debug("load() complete: no changes");
            }

            return changed;

        } catch (IOException e) {
            Instant now = Instant.now();
            status.lastErrorAt = now;
            status.lastError = e;
            if (status.firstErrorAt == null) status.firstErrorAt = now;
            log.error("load() failed", e);
            throw e;
        }
    }

    /**
     * Returns a snapshot of the currently cached file contents in index order.
     * The map is a defensive copy; mutations do not affect the loader's state.
     */
    public synchronized Map<String, String> getFileContents() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(fileContents));
    }

    /**
     * Returns the status object for this loader.
     * Individual fields are {@code volatile} and safe to read from any thread.
     */
    public LoadStatus getStatus() {
        return status;
    }

    // -------------------------------------------------------------------------
    // Push-notification and resource lifecycle
    // -------------------------------------------------------------------------

    private void doReloadOnChange() {
        try {
            load();
        } catch (IOException e) {
            log.error("Auto-reload triggered by source change listener failed", e);
        }
    }

    @Override
    public void close() throws IOException {
        source.close();
    }
}
