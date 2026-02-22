package org.csa.truffle.graal.source.file;

import org.csa.truffle.graal.source.PythonSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;

/**
 * {@link PythonSource} that reads Python files from a local directory and
 * watches it for changes via {@link WatchService}.
 *
 * <p>Construct with the directory path; {@link #setChangeListener(Runnable)} is
 * called automatically by {@link org.csa.truffle.graal.GraalPyInterpreter} and
 * starts a daemon watcher thread.
 *
 * <pre>
 *   try (var src = new FilePythonSource(Path.of("/opt/scripts/python"));
 *        var interp = new GraalPyInterpreter(src)) {
 *       interp.reload();
 *       // watcher now fires automatic reloads on file changes
 *   }
 * </pre>
 */
public class FilePythonSource implements PythonSource {

    private static final Logger log = LoggerFactory.getLogger(FilePythonSource.class);
    private static final long DEBOUNCE_MS = 100;

    private final Path directory;
    private volatile Runnable changeListener;
    private final boolean watch;
    private WatchService watchService;
    private Thread watcherThread;

    public FilePythonSource(Path directory, boolean watch) {
        this.directory = directory;
        this.watch = watch;
    }

    @Override
    public List<String> listFiles() throws IOException {
        Path index = directory.resolve("index.txt");
        return Files.readString(index, StandardCharsets.UTF_8).lines()
                .map(String::trim)
                .filter(l -> !l.isEmpty() && !l.startsWith("#"))
                .toList();
    }

    @Override
    public String readFile(String name) throws IOException {
        return Files.readString(directory.resolve(name), StandardCharsets.UTF_8);
    }

    /**
     * Called once by GraalPyInterpreter. Starts the watcher thread.
     */
    @Override
    public synchronized void setChangeListener(Runnable onChanged) {
        if (!watch || watcherThread != null) return; // idempotent
        this.changeListener = onChanged;
        log.info("Starting file watcher on: {}", directory);
        try {
            watchService = FileSystems.getDefault().newWatchService();
            directory.register(watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_DELETE);
        } catch (IOException e) {
            log.error("Could not start file watcher: {}", e.getMessage());
            return;
        }
        watcherThread = new Thread(this::watchLoop, "FilePythonSource-watcher");
        watcherThread.setDaemon(true);
        watcherThread.start();
        log.debug("Watcher thread started: {}", watcherThread.getName());
    }

    private void watchLoop() {
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

            boolean relevant = false;
            for (WatchEvent<?> event : key.pollEvents()) {
                Object ctx = event.context();
                if (ctx instanceof Path p) {
                    String name = p.getFileName().toString();
                    if (name.endsWith(".py") || name.equals("index.txt")) {
                        relevant = true;
                    }
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
}
