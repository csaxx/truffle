package org.csa.truffle.source.map;

import org.csa.truffle.source.FileSource;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link FileSource} backed by a thread-safe in-memory map.
 *
 * <p>Call {@link #put(String, String)} to add or overwrite a file and
 * {@link #remove(String)} to delete one; neither call automatically fires
 * the change listener, allowing callers to batch mutations.
 * Call {@link #triggerChange()} to explicitly push a reload notification.
 *
 * <p>Timestamps are recorded at each {@link #put} call so {@code FileLoader}
 * can detect changes efficiently.
 */
public class MapFileSource implements FileSource {

    private record Entry(String content, Instant modifiedAt) {}

    private final ConcurrentHashMap<String, Entry> map = new ConcurrentHashMap<>();
    private final String[] filemasks;
    private final String[] excludeFilemasks;
    private volatile Runnable changeListener;

    public MapFileSource(String[] filemasks, String[] excludeFilemasks) {
        this.filemasks = filemasks;
        this.excludeFilemasks = excludeFilemasks;
    }

    public MapFileSource(String[] filemasks) {
        this(filemasks, null);
    }

    public MapFileSource() {
        this(null, null);
    }

    /**
     * Adds or overwrites a file; records {@code Instant.now()} as its mtime.
     * Does <em>not</em> auto-trigger the change listener.
     */
    public void put(String name, String content) {
        map.put(name, new Entry(content, Instant.now()));
    }

    /**
     * Removes a file.
     * Does <em>not</em> auto-trigger the change listener.
     */
    public void remove(String name) {
        map.remove(name);
    }

    /**
     * Fires the registered change listener (no-op if none has been registered).
     * Allows callers to batch multiple mutations before notifying.
     */
    public void triggerChange() {
        Runnable listener = changeListener;
        if (listener != null) listener.run();
    }

    @Override
    public Map<String, Optional<Instant>> listFiles() {
        PathMatcher[] matchers = buildMatchers(filemasks);
        PathMatcher[] excludeMatchers = buildMatchers(excludeFilemasks);
        // snapshot to avoid ConcurrentModificationException
        List<Map.Entry<String, Entry>> snapshot = new ArrayList<>(map.entrySet());
        snapshot.sort(Map.Entry.comparingByKey());

        LinkedHashMap<String, Optional<Instant>> result = new LinkedHashMap<>();
        for (Map.Entry<String, Entry> e : snapshot) {
            String name = e.getKey();
            if (matchesAnyExclude(name, excludeMatchers)) continue;
            if (matchesMasks(name, matchers)) {
                result.put(name, Optional.of(e.getValue().modifiedAt()));
            }
        }
        return result;
    }

    @Override
    public String readFile(String name) throws IOException {
        Entry entry = map.get(name);
        if (entry == null) throw new IOException("File not found: " + name);
        return entry.content();
    }

    @Override
    public void setChangeListener(Runnable onChanged) {
        this.changeListener = onChanged;
    }

    // -------------------------------------------------------------------------
    // Helpers (copied from ResourceSource — package-private there)
    // -------------------------------------------------------------------------

    static PathMatcher[] buildMatchers(String[] filemasks) {
        if (filemasks == null || filemasks.length == 0) return null;
        PathMatcher[] matchers = new PathMatcher[filemasks.length];
        for (int i = 0; i < filemasks.length; i++) {
            matchers[i] = FileSystems.getDefault().getPathMatcher("glob:" + filemasks[i]);
        }
        return matchers;
    }

    static boolean matchesMasks(String name, PathMatcher[] matchers) {
        if (matchers == null) return true;
        Path filename = Path.of(name).getFileName();
        if (filename == null) return false;
        for (PathMatcher matcher : matchers) {
            if (matcher.matches(filename)) return true;
        }
        return false;
    }

    /**
     * Returns {@code true} if any exclude pattern matches any component of {@code relativePath}.
     */
    static boolean matchesAnyExclude(String relativePath, PathMatcher[] excludeMatchers) {
        if (excludeMatchers == null) return false;
        for (Path component : Path.of(relativePath)) {
            for (PathMatcher matcher : excludeMatchers) {
                if (matcher.matches(component)) return true;
            }
        }
        return false;
    }
}
