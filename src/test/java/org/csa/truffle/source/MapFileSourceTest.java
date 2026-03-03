package org.csa.truffle.source;

import org.csa.truffle.loader.FileLoader;
import org.csa.truffle.loader.LoadResult;
import org.csa.truffle.source.map.MapFileSource;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MapFileSourceTest {

    @Test
    void listFiles_emptySource_returnsEmpty() throws IOException {
        MapFileSource src = new MapFileSource();
        assertTrue(src.listFiles().isEmpty());
    }

    @Test
    void listFiles_returnsAddedFiles_alphabetically() throws IOException {
        MapFileSource src = new MapFileSource();
        src.put("zebra.py", "z");
        src.put("alpha.py", "a");
        src.put("middle.py", "m");

        Map<String, Optional<Instant>> files = src.listFiles();
        assertIterableEquals(java.util.List.of("alpha.py", "middle.py", "zebra.py"), files.keySet());
    }

    @Test
    void listFiles_returnsTimestamps() throws IOException {
        MapFileSource src = new MapFileSource();
        src.put("script.py", "content");

        Map<String, Optional<Instant>> files = src.listFiles();
        assertTrue(files.get("script.py").isPresent());
    }

    @Test
    void listFiles_filemaskFiltersFiles() throws IOException {
        MapFileSource src = new MapFileSource(new String[]{"*.py"});
        src.put("transform.py", "py content");
        src.put("readme.txt", "text content");

        Map<String, Optional<Instant>> files = src.listFiles();
        assertTrue(files.containsKey("transform.py"));
        assertFalse(files.containsKey("readme.txt"));
    }

    @Test
    void listFiles_excludeFilemaskExcludesFile() throws IOException {
        MapFileSource src = new MapFileSource(null, new String[]{"excluded.py"});
        src.put("keep.py", "keep");
        src.put("excluded.py", "excluded");
        src.put("also_keep.py", "also keep");

        Map<String, Optional<Instant>> files = src.listFiles();
        assertTrue(files.containsKey("keep.py"));
        assertTrue(files.containsKey("also_keep.py"));
        assertFalse(files.containsKey("excluded.py"));
    }

    @Test
    void readFile_returnsContent() throws IOException {
        MapFileSource src = new MapFileSource();
        src.put("script.py", "hello world");
        assertEquals("hello world", src.readFile("script.py"));
    }

    @Test
    void readFile_throwsOnMissingFile() {
        MapFileSource src = new MapFileSource();
        assertThrows(IOException.class, () -> src.readFile("nonexistent.py"));
    }

    @Test
    void put_overwritesExistingFile() throws IOException, InterruptedException {
        MapFileSource src = new MapFileSource();
        src.put("script.py", "v1");
        Instant first = src.listFiles().get("script.py").orElseThrow();

        Thread.sleep(2); // ensure clock advances
        src.put("script.py", "v2");

        assertEquals("v2", src.readFile("script.py"));
        Instant second = src.listFiles().get("script.py").orElseThrow();
        assertFalse(second.isBefore(first));
    }

    @Test
    void remove_removesFile() throws IOException {
        MapFileSource src = new MapFileSource();
        src.put("script.py", "content");
        src.remove("script.py");

        assertFalse(src.listFiles().containsKey("script.py"));
        assertThrows(IOException.class, () -> src.readFile("script.py"));
    }

    @Test
    void triggerChange_callsListener() {
        MapFileSource src = new MapFileSource();
        AtomicInteger count = new AtomicInteger();
        src.setChangeListener(count::incrementAndGet);

        src.triggerChange();

        assertEquals(1, count.get());
    }

    @Test
    void triggerChange_noListener_doesNotThrow() {
        MapFileSource src = new MapFileSource();
        assertDoesNotThrow(src::triggerChange);
    }

    @Test
    void put_doesNotAutoTrigger() {
        MapFileSource src = new MapFileSource();
        AtomicInteger count = new AtomicInteger();
        src.setChangeListener(count::incrementAndGet);

        src.put("script.py", "content");

        assertEquals(0, count.get());
    }

    @Test
    void remove_doesNotAutoTrigger() {
        MapFileSource src = new MapFileSource();
        src.put("script.py", "content");
        AtomicInteger count = new AtomicInteger();
        src.setChangeListener(count::incrementAndGet);

        src.remove("script.py");

        assertEquals(0, count.get());
    }

    @Test
    void integration_withFileLoader() throws Exception {
        MapFileSource src = new MapFileSource(new String[]{"*.py"});
        src.put("transform.py", "v1");

        AtomicInteger reloadCount = new AtomicInteger();
        try (FileLoader loader = new FileLoader(src, (LoadResult r) -> {
            if (r.success()) reloadCount.incrementAndGet();
        })) {
            // Initial load
            LoadResult first = loader.load();
            assertTrue(first.success());
            assertEquals("v1", loader.getFileContents().get("transform.py"));
            assertEquals(1, reloadCount.get());

            // Sleep so the next put() records a strictly later mtime than the one stored by load()
            Thread.sleep(2);

            // Mutate and trigger
            src.put("transform.py", "v2");
            src.triggerChange(); // fires FileLoader.load() automatically

            assertEquals("v2", loader.getFileContents().get("transform.py"));
            assertEquals(2, reloadCount.get());
        }
    }
}
