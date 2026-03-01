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
 * <p>Files under any {@code venv/} subtree are excluded. The {@code filemask}
 * glob is matched against the filename (last path component); pass {@code null}
 * to include all files.
 */
public class FileSystemSource implements FileSource {

    private static final Logger log = LoggerFactory.getLogger(FileSystemSource.class);
    private static final long DEBOUNCE_MS = 100;

    private final Path directory;
    private final boolean watch;
    private final String filemask;
    private volatile Runnable changeListener;
    private WatchService watchService;
    private Thread watcherThread;
    private final Map<WatchKey, Path> watchedDirs = new ConcurrentHashMap<>();

    public FileSystemSource(Path directory, boolean watch, String filemask) {
        this.directory = directory;
        this.watch = watch;
        this.filemask = filemask;
    }

    public FileSystemSource(Path directory, boolean watch) {
        this(directory, watch, null);
    }

    @Override
    public Map<String, Optional<Instant>> listFiles() throws IOException {
        PathMatcher matcher = buildMatcher(filemask);
        LinkedHashMap<String, Optional<Instant>> result = new LinkedHashMap<>();
        try (Stream<Path> walk = Files.walk(directory)) {
            walk
                    .filter(Files::isRegularFile)
                    .filter(p -> !isVenvPath(directory.relativize(p)))
                    .filter(p -> matchesMask(directory.relativize(p).toString().replace('\\', '/'), matcher))
                    .sorted(Comparator.comparing(
                            p -> directory.relativize(p).toString().replace('\\', '/')))
                    .forEach(file -> {
                        String rel = directory.relativize(file).toString().replace('\\', '/');
                        Instant mtime;
                        try {
                            mtime = Files.getLastModifiedTime(file).toInstant();
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
        PathMatcher matcher = buildMatcher(filemask);
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
                if (matchesMask(name, matcher)) {
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

    static boolean isVenvPath(Path relativePath) {
        for (Path component : relativePath) {
            if ("venv".equals(component.toString())) return true;
        }
        return false;
    }

    static PathMatcher buildMatcher(String filemask) {
        if (filemask == null) return null;
        return FileSystems.getDefault().getPathMatcher("glob:" + filemask);
    }

    static boolean matchesMask(String name, PathMatcher matcher) {
        if (matcher == null) return true;
        Path filename = Path.of(name).getFileName();
        return filename != null && matcher.matches(filename);
    }
}
