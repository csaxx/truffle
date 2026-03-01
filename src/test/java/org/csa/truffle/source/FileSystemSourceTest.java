package org.csa.truffle.source;

import org.csa.truffle.source.file.FileSystemSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class FileSystemSourceTest {

    @TempDir
    Path tempDir;

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private void writePy(String relative, String content) throws IOException {
        Path target = tempDir.resolve(relative);
        Files.createDirectories(target.getParent());
        Files.writeString(target, content);
    }

    // -------------------------------------------------------------------------
    // Basic I/O
    // -------------------------------------------------------------------------

    @Test
    void listFiles_returnsAllFilesAlphabetically() throws IOException {
        writePy("b.py", "");
        writePy("a.py", "");
        FileSystemSource src = new FileSystemSource(tempDir, false);
        Map<String, Optional<Instant>> files = src.listFiles();
        assertIterableEquals(List.of("a.py", "b.py"), files.keySet());
    }

    @Test
    void listFiles_returnsAllFilesRecursively() throws IOException {
        writePy("top.py", "");
        writePy("sub/nested.py", "");
        FileSystemSource src = new FileSystemSource(tempDir, false);
        Map<String, Optional<Instant>> files = src.listFiles();
        assertIterableEquals(List.of("sub/nested.py", "top.py"), files.keySet());
    }

    @Test
    void listFiles_skipsVenvDirectory() throws IOException {
        writePy("keep.py", "");
        writePy("venv/site-packages/lib.py", "");
        FileSystemSource src = new FileSystemSource(tempDir, false);
        Map<String, Optional<Instant>> files = src.listFiles();
        assertTrue(files.containsKey("keep.py"));
        assertFalse(files.keySet().stream().anyMatch(k -> k.contains("venv")));
    }

    @Test
    void listFiles_filemaskFiltersExtension() throws IOException {
        writePy("a.py", "");
        Files.writeString(tempDir.resolve("notes.txt"), "ignored");
        FileSystemSource src = new FileSystemSource(tempDir, false, new String[]{"*.py"});
        Map<String, Optional<Instant>> files = src.listFiles();
        assertTrue(files.containsKey("a.py"));
        assertFalse(files.containsKey("notes.txt"));
    }

    @Test
    void listFiles_returnsMtimeForExistingFiles() throws IOException {
        writePy("a.py", "content");
        FileSystemSource src = new FileSystemSource(tempDir, false);
        Map<String, Optional<Instant>> files = src.listFiles();
        assertTrue(files.get("a.py").isPresent(), "mtime should be present for existing file");
    }

    @Test
    void listFiles_throwsWhenDirectoryMissing() {
        FileSystemSource src = new FileSystemSource(tempDir.resolve("no_such_dir"), false);
        assertThrows(IOException.class, src::listFiles);
    }

    @Test
    void readFile_returnsContent() throws IOException {
        writePy("transform.py", "def process_element(line, out): pass");
        FileSystemSource src = new FileSystemSource(tempDir, false);
        String content = src.readFile("transform.py");
        assertEquals("def process_element(line, out): pass", content);
    }

    @Test
    void readFile_throwsOnMissingFile() {
        FileSystemSource src = new FileSystemSource(tempDir, false);
        assertThrows(IOException.class, () -> src.readFile("no_such.py"));
    }

    // -------------------------------------------------------------------------
    // Watch mode
    // -------------------------------------------------------------------------

    @Test
    void watch_false_setChangeListenerIsNoop() throws IOException {
        writePy("a.py", "");
        FileSystemSource src = new FileSystemSource(tempDir, false);
        src.setChangeListener(() -> fail("should not be called"));
        boolean watcherExists = Thread.getAllStackTraces().keySet().stream()
                .anyMatch(t -> t.getName().contains("FileSource-watcher"));
        assertFalse(watcherExists);
        src.close();
    }

    @Test
    void watch_true_listenerCalledOnPyFileModify() throws Exception {
        writePy("a.py", "v1");

        CountDownLatch latch = new CountDownLatch(1);
        try (FileSystemSource src = new FileSystemSource(tempDir, true, new String[]{"*.py"})) {
            src.setChangeListener(latch::countDown);
            Thread.sleep(50); // let watcher register
            writePy("a.py", "v2");
            assertTrue(latch.await(5, TimeUnit.SECONDS), "listener not called after .py file change");
        }
    }

    @Test
    void watch_true_listenerCalledOnSubdirFileModify() throws Exception {
        writePy("sub/a.py", "v1");

        CountDownLatch latch = new CountDownLatch(1);
        try (FileSystemSource src = new FileSystemSource(tempDir, true, new String[]{"*.py"})) {
            src.setChangeListener(latch::countDown);
            Thread.sleep(50);
            writePy("sub/a.py", "v2");
            assertTrue(latch.await(5, TimeUnit.SECONDS), "listener not called after subdir .py change");
        }
    }

    @Test
    void watch_true_unrelatedFileIgnored() throws Exception {
        writePy("a.py", "v1");

        CountDownLatch latch = new CountDownLatch(1);
        try (FileSystemSource src = new FileSystemSource(tempDir, true, new String[]{"*.py"})) {
            src.setChangeListener(latch::countDown);
            Thread.sleep(50);
            Files.writeString(tempDir.resolve("notes.txt"), "ignored");
            assertFalse(latch.await(500, TimeUnit.MILLISECONDS),
                    "listener should not be called for unrelated file");
        }
    }

    @Test
    void setChangeListener_idempotent() throws Exception {
        writePy("a.py", "v1");

        CountDownLatch latch = new CountDownLatch(1);
        try (FileSystemSource src = new FileSystemSource(tempDir, true, new String[]{"*.py"})) {
            src.setChangeListener(latch::countDown);
            src.setChangeListener(() -> fail("second listener should not replace first"));
            Thread.sleep(50);
            writePy("a.py", "v2");
            assertTrue(latch.await(5, TimeUnit.SECONDS), "first listener should still fire");
        }
    }

    @Test
    void close_stopsWatcherThread() throws Exception {
        writePy("a.py", "v1");

        FileSystemSource src = new FileSystemSource(tempDir, true);
        src.setChangeListener(() -> {});
        Thread.sleep(50);

        Thread watcher = Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().contains("FileSource-watcher"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("watcher thread not found"));

        src.close();
        watcher.join(3000);
        assertFalse(watcher.isAlive(), "watcher thread should have stopped after close()");
    }
}
