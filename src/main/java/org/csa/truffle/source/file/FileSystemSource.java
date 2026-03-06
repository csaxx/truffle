package org.csa.truffle.source.file;

import org.csa.truffle.source.FileSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * {@link FileSource} that auto-discovers files by walking a local directory and
 * watches it recursively for changes via {@link WatchService}.
 *
 * <p>Files matching any {@code excludeFilemasks} pattern (matched against each path component)
 * are excluded. Each of the {@code filemasks} globs is matched against the filename (last path
 * component); a file matches if it matches any pattern. Pass {@code null} or an empty array to
 * include all files.
 */
public class FileSystemSource implements FileSource {

    private static final Logger log = LoggerFactory.getLogger(FileSystemSource.class);
    private static final long DEBOUNCE_MS = 100;

    private final Path directory;
    private final boolean watch;
    private final String[] filemasks;
    private final String[] excludeFilemasks;
    private volatile Runnable changeListener;
    private WatchService watchService;
    private Thread watcherThread;
    private final Map<WatchKey, Path> watchedDirs = new ConcurrentHashMap<>();

    public FileSystemSource(FileSystemSourceConfig config) {
        this.directory = Path.of(config.directory());
        this.watch = config.watch();
        this.filemasks = config.filemasks();
        this.excludeFilemasks = config.excludeFilemasks();
    }

    @Override
    public Map<String, Optional<Instant>> listFiles() throws IOException {
        PathMatcher[] matchers = buildMatchers(filemasks);
        PathMatcher[] excludeMatchers = buildMatchers(excludeFilemasks);
        LinkedHashMap<String, Optional<Instant>> result = new LinkedHashMap<>();
        try (Stream<Path> walk = Files.walk(directory)) {
            walk
                    .filter(Files::isRegularFile)
                    .map(p -> directory.relativize(p).toString().replace('\\', '/'))
                    .filter(rel -> !matchesAnyExclude(rel, excludeMatchers))
                    .filter(rel -> matchesMasks(rel, matchers))
                    .sorted(Comparator.naturalOrder())
                    .forEach(rel -> {
                        Instant mtime;
                        try {
                            mtime = Files.getLastModifiedTime(directory.resolve(rel)).toInstant();
                        } catch (IOException e) {
                            mtime = null;
                        }
                        result.put(rel, Optional.ofNullable(mtime));
                    });
        }
        return result;
    }

    @Override
    public String readFile(String name) throws IOException {
        return Files.readString(directory.resolve(name), StandardCharsets.UTF_8);
    }

    @Override
    public synchronized void setChangeListener(Runnable onChanged) {
        if (!watch || watcherThread != null) return; // idempotent
        this.changeListener = onChanged;
        log.info("Starting file watcher on: {}", directory);
        try {
            watchService = FileSystems.getDefault().newWatchService();
            registerTree(directory);
        } catch (IOException e) {
            log.error("Could not start file watcher: {}", e.getMessage());
            return;
        }
        watcherThread = new Thread(this::watchLoop, "FileSource-watcher");
        watcherThread.setDaemon(true);
        watcherThread.start();
        log.debug("Watcher thread started: {}", watcherThread.getName());
    }

    private void registerTree(Path dir) throws IOException {
        WatchKey key = dir.register(watchService,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.ENTRY_DELETE);
        watchedDirs.put(key, dir);
        // Register all existing subdirectories recursively
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.filter(Files::isDirectory)
                    .filter(p -> !p.equals(dir))
                    .forEach(subdir -> {
                        try {
                            WatchKey k = subdir.register(watchService,
                                    StandardWatchEventKinds.ENTRY_CREATE,
                                    StandardWatchEventKinds.ENTRY_MODIFY,
                                    StandardWatchEventKinds.ENTRY_DELETE);
                            watchedDirs.put(k, subdir);
                        } catch (IOException e) {
                            log.warn("Could not register subdirectory for watching: {}", subdir);
                        }
                    });
        }
    }

    private void watchLoop() {
        PathMatcher[] matchers = buildMatchers(filemasks);
        while (!Thread.currentThread().isInterrupted()) {
            WatchKey key;
            try {
                key = watchService.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (ClosedWatchServiceException e) {
                return;
            }

            Path watchedDir = watchedDirs.get(key);
            boolean relevant = false;

            for (WatchEvent<?> event : key.pollEvents()) {
                Object ctx = event.context();
                if (!(ctx instanceof Path p)) continue;

                Path fullPath = watchedDir != null ? watchedDir.resolve(p) : p;

                if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE
                        && Files.isDirectory(fullPath)) {
                    // New subdirectory: register it for watching
                    try {
                        WatchKey k = fullPath.register(watchService,
                                StandardWatchEventKinds.ENTRY_CREATE,
                                StandardWatchEventKinds.ENTRY_MODIFY,
                                StandardWatchEventKinds.ENTRY_DELETE);
                        watchedDirs.put(k, fullPath);
                    } catch (IOException e) {
                        log.warn("Could not register new directory: {}", fullPath);
                    }
                    relevant = true;
                    continue;
                }

                String name = p.getFileName().toString();
                if (matchesMasks(name, matchers)) {
                    relevant = true;
                }
            }
            key.reset();

            if (relevant) {
                try {
                    Thread.sleep(DEBOUNCE_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                log.info("File system change detected in {}; invoking reload callback", directory);
                Runnable listener = changeListener;
                if (listener != null) listener.run();
            }
        }
    }

    @Override
    public void close() throws IOException {
        log.debug("Closing file watcher for: {}", directory);
        if (watcherThread != null) watcherThread.interrupt();
        if (watchService != null) watchService.close();
    }

    // -------------------------------------------------------------------------
    // Shared helpers (duplicated from ResourceSource for package isolation)
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
