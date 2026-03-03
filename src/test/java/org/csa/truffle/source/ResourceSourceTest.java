package org.csa.truffle.source;

import org.csa.truffle.source.resource.ResourceSource;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class ResourceSourceTest {

    // python_hr_v1 has: file_in_both_changed.py, file_in_both_unchanged.py, file_only_in_v1.py
    private static final String DIR = "python_hr_v1";

    @Test
    void listFiles_returnsFilesAlphabetically() throws IOException {
        ResourceSource src = new ResourceSource(DIR);
        Map<String, Optional<Instant>> files = src.listFiles();
        List<String> keys = List.copyOf(files.keySet());
        assertEquals(List.of(
                "file_in_both_changed.py",
                "file_in_both_unchanged.py",
                "file_only_in_v1.py"), keys);
    }

    @Test
    void listFiles_allTimestampsAreEmpty() throws IOException {
        ResourceSource src = new ResourceSource(DIR);
        Map<String, Optional<Instant>> files = src.listFiles();
        for (Optional<Instant> ts : files.values()) {
            assertTrue(ts.isEmpty(), "expected empty timestamp for classpath resource");
        }
    }

    @Test
    void listFiles_filemaskFiltersExtension() throws IOException {
        // python_hr_with_comments contains only file_in_both_unchanged.py
        ResourceSource src = new ResourceSource("python_hr_with_comments", new String[]{"*.py"});
        Map<String, Optional<Instant>> files = src.listFiles();
        assertEquals(1, files.size());
        assertTrue(files.containsKey("file_in_both_unchanged.py"));
    }

    @Test
    void readFile_returnsCorrectContent() throws IOException {
        ResourceSource src = new ResourceSource(DIR);
        String content = src.readFile("file_only_in_v1.py");
        assertTrue(content.contains("v1_only"), "expected v1_only marker in file content");
    }

    @Test
    void listFiles_throwsOnMissingDirectory() {
        ResourceSource src = new ResourceSource("no_such_directory");
        assertThrows(IOException.class, src::listFiles);
    }

    @Test
    void readFile_throwsOnMissingFile() {
        ResourceSource src = new ResourceSource(DIR);
        assertThrows(IOException.class, () -> src.readFile("no_such_file.py"));
    }

    @Test
    void listFiles_excludeFilemaskExcludesFile() throws IOException {
        // python_hr_v1 has: file_in_both_changed.py, file_in_both_unchanged.py, file_only_in_v1.py
        ResourceSource src = new ResourceSource(DIR, null, new String[]{"file_in_both_unchanged.py"});
        Map<String, Optional<Instant>> files = src.listFiles();
        assertFalse(files.containsKey("file_in_both_unchanged.py"), "excluded file should not appear");
        assertTrue(files.containsKey("file_in_both_changed.py"));
        assertTrue(files.containsKey("file_only_in_v1.py"));
    }
}
