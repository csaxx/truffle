package org.csa.truffle.loader;

import org.apache.commons.lang3.StringUtils;
import org.csa.truffle.loader.FileChangeInfo.ChangeStatus;
import org.csa.truffle.source.FileSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Instant;
import java.util.*;

/**
 * Loads and caches the contents of files from a {@link FileSource},
 * using per-file modification timestamps to avoid re-reading unchanged files.
 *
 * <p>Call {@link #load()} to (re)load from the source. On the first call every
 * listed file is read. On subsequent calls a file's content is re-read only
 * when its modification time has advanced since the last load; files for which
 * the source cannot provide a timestamp are always re-read. Files that
 * disappear are evicted from the cache.
 *
 * <p>An optional {@link LoadCallback} supplied at construction time is invoked
 * after every {@link #load()} attempt.
 *
 * <p>Operational state is tracked in a {@link FileLoaderStatus} instance;
 * obtain it via {@link #getStatus()}.
 */
public class FileLoader implements Closeable {

    /**
     * Callback fired by {@link FileLoader#load()} after each attempt.
     */
    @FunctionalInterface
    public interface LoadCallback {
        void notify(LoadResult result);
    }

    private static final Logger log = LoggerFactory.getLogger(FileLoader.class);

    private final FileSource source;

    /**
     * Optional callback for {@link LoadResult} on {@link #load()}.
     */
    private final LoadCallback callback;

    /**
     * Ordered cache: filename → current content. Preserved across calls.
     */
    private final Map<String, String> fileContents = new LinkedHashMap<>();

    /**
     * Per-file last-known modification time. Absent when a file was loaded without a timestamp.
     */
    private final Map<String, Instant> modTimes = new HashMap<>();

    private final FileLoaderStatus status = new FileLoaderStatus();

    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    /**
     * Creates a loader without a callback.
     */
    public FileLoader(FileSource source) {
        this(source, null);
    }

    /**
     * Creates a loader with an optional callback.
     *
     * @param source   the source to load files from
     * @param callback invoked after every {@link #load()} attempt; {@code null} to disable
     */
    public FileLoader(FileSource source, LoadCallback callback) {
        this.source = source;
        this.callback = callback;

        // source may provide a callback when input changes
        source.setChangeListener(this::load);
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
     * <p>{@link FileLoaderStatus} is updated on every call regardless of outcome.
     * This method never throws; I/O errors are captured in the returned
     * {@link LoadResult} and forwarded to the {@link LoadCallback} (if set).
     *
     * @return a {@link LoadResult} describing the outcome; success is
     * {@code true} when no I/O error occurred
     */
    public synchronized LoadResult load() {

        Instant checkedAt = Instant.now();
        status.lastCheckedAt = checkedAt;
        LoadResult result;

        try {
            Map<String, Optional<Instant>> fileList = source.listFiles();
            log.debug("load() started; source lists {} file(s)", fileList.size());

            boolean changed = false;

            List<FileChangeInfo> changes = new ArrayList<>();
            Map<String, String> newFileContents = new LinkedHashMap<>();
            Map<String, Instant> newModTimes = new HashMap<>();

            for (Map.Entry<String, Optional<Instant>> entry : fileList.entrySet()) {
                String filePath = entry.getKey();
                Optional<Instant> modTime = entry.getValue();

                // file needs to be (re)read if it is newer or has no modification time
                boolean needsRead;
                if (modTime.isPresent()) {
                    Instant lastKnown = modTimes.get(filePath);
                    needsRead = lastKnown == null || modTime.get().isAfter(lastKnown);
                    newModTimes.put(filePath, modTime.get());
                } else {
                    needsRead = true; // no timestamp available — always re-read
                }

                // read file (if necessary) and get contents
                ChangeStatus changeStatus;
                String content;
                if (needsRead) {
                    content = source.readFile(filePath);
                    String previous = fileContents.get(filePath);
                    if (previous == null) {
                        changeStatus = ChangeStatus.ADDED;
                        log.info("New file loaded: {}", filePath);
                        changed = true;
                    } else if (StringUtils.equals(content, previous)) {
                        changeStatus = ChangeStatus.MODIFIED;
                        log.info("File content updated: {}", filePath);
                        changed = true;
                    } else {
                        changeStatus = ChangeStatus.UNMODIFIED;
                        log.debug("File re-read but unchanged: {}", filePath);
                    }
                } else {
                    content = fileContents.get(filePath); // reuse from cache
                    changeStatus = ChangeStatus.UNMODIFIED;
                    log.debug("Skipped unchanged file (modTime): {}", filePath);
                }

                newFileContents.put(filePath, content);
                changes.add(new FileChangeInfo(filePath, modTime, changeStatus));
            }

            // Detect removed files
            for (String filePath : fileContents.keySet()) {
                if (!newFileContents.containsKey(filePath)) {
                    log.info("File removed from index: {}", filePath);
                    changes.add(new FileChangeInfo(filePath, Optional.empty(), ChangeStatus.REMOVED));
                    changed = true;
                }
            }

            // update current file contents/mod times
            fileContents.clear();
            fileContents.putAll(newFileContents);
            modTimes.clear();
            modTimes.putAll(newModTimes);

            Optional<Instant> maxDataAge = fileList.values().stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .max(Comparator.naturalOrder());

            // update status
            if (changed) {
                status.lastChangedAt = checkedAt;
                log.debug("load() complete: change(s) detected");
            } else {
                log.debug("load() complete: no changes");
            }

            status.lastSuccessAt = checkedAt;
            status.lastDataAge = maxDataAge.orElse(null);
            status.loadedFiles = Set.copyOf(newFileContents.keySet());

            List<FileChangeInfo> changesCopy = List.copyOf(changes);
            result = new LoadResult(status, changesCopy);

        } catch (Exception e) {

            log.error("load() failed", e);

            // update status
            status.lastErrorAt = checkedAt;
            status.lastError = e;

            result = new LoadResult(status, e);
        }

        if (callback != null) {
            try {
                callback.notify(result);
            } catch (Exception e) {
                log.warn("error notifying callback", e);
            }
        }

        return result;
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
    public FileLoaderStatus getStatus() {
        return status;
    }

    @Override
    public void close() {
        try {
            source.close();
        } catch (Exception ignored) {
        }
    }

}
