package org.csa.truffle.loader;

import org.csa.truffle.loader.result.ChangeStatus;
import org.csa.truffle.loader.result.LoadResult;
import org.csa.truffle.source.FileSource;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class FileLoaderTest {

    // -------------------------------------------------------------------------
    // Inner helper classes
    // -------------------------------------------------------------------------

    /**
     * A source whose {@code listFiles()} always throws {@link IOException}.
     */
    static class FailingSource implements FileSource {
        @Override
        public Map<String, Optional<Instant>> listFiles() throws IOException {
            throw new IOException("intentional failure");
        }

        @Override
        public String readFile(String name) throws IOException {
            throw new IOException("intentional failure");
        }
    }

    /**
     * Wraps {@link SwitchableFileSource}, captures the change listener registered by
     * {@link FileLoader}, and exposes {@link #triggerChange()} for synchronous push tests.
     */
    static class NotifyingSource implements FileSource {
        private final SwitchableFileSource delegate;
        private Runnable listener;

        NotifyingSource(String initialDir) {
            delegate = new SwitchableFileSource(initialDir);
        }

        @Override
        public void setChangeListener(Runnable r) {
            listener = r;
        }

        public void triggerChange() {
            if (listener != null) listener.run();
        }

        public void switchTo(String dir) {
            delegate.switchTo(dir);
        }

        @Override
        public Map<String, Optional<Instant>> listFiles() throws IOException {
            return delegate.listFiles();
        }

        @Override
        public String readFile(String name) throws IOException {
            return delegate.readFile(name);
        }
    }

    // -------------------------------------------------------------------------
    // Case 1: file present in v1 but not v2 (removed on reload)
    // -------------------------------------------------------------------------

    @Test
    void case1_fileOnlyInV1_presentBeforeReload() throws Exception {
        try (FileLoader loader = new FileLoader(new SwitchableFileSource("python_hr_v1"))) {
            loader.load();
            assertTrue(loader.getFileContents().containsKey("file_only_in_v1.py"));
        }
    }

    @Test
    void case1_fileOnlyInV1_absentAfterReload() throws Exception {
        SwitchableFileSource src = new SwitchableFileSource("python_hr_v1");
        try (FileLoader loader = new FileLoader(src)) {
            loader.load();
            src.switchTo("python_hr_v2");
            loader.load();
            assertFalse(loader.getFileContents().containsKey("file_only_in_v1.py"));
        }
    }

    @Test
    void case1_fileOnlyInV1_contentPresentBeforeReload() throws Exception {
        try (FileLoader loader = new FileLoader(new SwitchableFileSource("python_hr_v1"))) {
            loader.load();
            assertTrue(loader.getFileContents().values().stream()
                    .anyMatch(c -> c.contains("v1_only")));
        }
    }

    @Test
    void case1_fileOnlyInV1_contentAbsentAfterReload() throws Exception {
        SwitchableFileSource src = new SwitchableFileSource("python_hr_v1");
        try (FileLoader loader = new FileLoader(src)) {
            loader.load();
            src.switchTo("python_hr_v2");
            loader.load();
            assertFalse(loader.getFileContents().values().stream()
                    .anyMatch(c -> c.contains("v1_only")));
        }
    }

    // -------------------------------------------------------------------------
    // Case 2: file absent in v1 but present in v2 (added on reload)
    // -------------------------------------------------------------------------

    @Test
    void case2_fileOnlyInV2_absentBeforeReload() throws Exception {
        try (FileLoader loader = new FileLoader(new SwitchableFileSource("python_hr_v1"))) {
            loader.load();
            assertFalse(loader.getFileContents().containsKey("file_only_in_v2.py"));
        }
    }

    @Test
    void case2_fileOnlyInV2_presentAfterReload() throws Exception {
        SwitchableFileSource src = new SwitchableFileSource("python_hr_v1");
        try (FileLoader loader = new FileLoader(src)) {
            loader.load();
            src.switchTo("python_hr_v2");
            loader.load();
            assertTrue(loader.getFileContents().containsKey("file_only_in_v2.py"));
        }
    }

    @Test
    void case2_fileOnlyInV2_contentAbsentBeforeReload() throws Exception {
        try (FileLoader loader = new FileLoader(new SwitchableFileSource("python_hr_v1"))) {
            loader.load();
            assertFalse(loader.getFileContents().values().stream()
                    .anyMatch(c -> c.contains("v2_only")));
        }
    }

    @Test
    void case2_fileOnlyInV2_contentPresentAfterReload() throws Exception {
        SwitchableFileSource src = new SwitchableFileSource("python_hr_v1");
        try (FileLoader loader = new FileLoader(src)) {
            loader.load();
            src.switchTo("python_hr_v2");
            loader.load();
            assertTrue(loader.getFileContents().values().stream()
                    .anyMatch(c -> c.contains("v2_only")));
        }
    }

    // -------------------------------------------------------------------------
    // Case 3: file present in both with identical content
    // -------------------------------------------------------------------------

    @Test
    void case3_unchangedFile_presentInBoth() throws Exception {
        SwitchableFileSource src = new SwitchableFileSource("python_hr_v1");
        try (FileLoader loader = new FileLoader(src)) {
            loader.load();
            assertTrue(loader.getFileContents().containsKey("file_in_both_unchanged.py"));
            src.switchTo("python_hr_v2");
            loader.load();
            assertTrue(loader.getFileContents().containsKey("file_in_both_unchanged.py"));
        }
    }

    @Test
    void case3_unchangedFile_sameContentInBoth() throws Exception {
        SwitchableFileSource src = new SwitchableFileSource("python_hr_v1");
        try (FileLoader loader = new FileLoader(src)) {
            loader.load();
            String v1Content = loader.getFileContents().get("file_in_both_unchanged.py");
            src.switchTo("python_hr_v2");
            loader.load();
            String v2Content = loader.getFileContents().get("file_in_both_unchanged.py");
            assertEquals(v1Content, v2Content);
        }
    }

    // -------------------------------------------------------------------------
    // Case 4: file present in both with different content
    // -------------------------------------------------------------------------

    @Test
    void case4_changedFile_presentInBoth() throws Exception {
        SwitchableFileSource src = new SwitchableFileSource("python_hr_v1");
        try (FileLoader loader = new FileLoader(src)) {
            loader.load();
            assertTrue(loader.getFileContents().containsKey("file_in_both_changed.py"));
            src.switchTo("python_hr_v2");
            loader.load();
            assertTrue(loader.getFileContents().containsKey("file_in_both_changed.py"));
        }
    }

    @Test
    void case4_changedFile_differentContentAfterReload() throws Exception {
        SwitchableFileSource src = new SwitchableFileSource("python_hr_v1");
        try (FileLoader loader = new FileLoader(src)) {
            loader.load();
            assertTrue(loader.getFileContents().get("file_in_both_changed.py").contains("changed_v1"));
            src.switchTo("python_hr_v2");
            loader.load();
            assertTrue(loader.getFileContents().get("file_in_both_changed.py").contains("changed_v2"));
        }
    }

    // -------------------------------------------------------------------------
    // load() return value
    // -------------------------------------------------------------------------

    @Test
    void loadReturnsTrueWhenFilesChange() throws Exception {
        SwitchableFileSource src = new SwitchableFileSource("python_hr_v1");
        try (FileLoader loader = new FileLoader(src)) {
            loader.load();
            Instant firstChangedAt = loader.getStatus().getLastChangedAt();
            src.switchTo("python_hr_v2");
            LoadResult result = loader.load();
            assertTrue(result.success());
            assertTrue(result.files().stream().anyMatch(c -> c.status() != ChangeStatus.UNMODIFIED));
            assertTrue(loader.getStatus().getLastChangedAt().isAfter(firstChangedAt));
        }
    }

    @Test
    void loadReturnsFalseWhenUnchanged() throws Exception {
        SwitchableFileSource src = new SwitchableFileSource("python_hr_v1");
        try (FileLoader loader = new FileLoader(src)) {
            loader.load();
            Instant firstChangedAt = loader.getStatus().getLastChangedAt();
            LoadResult result = loader.load();
            assertTrue(result.success());
            assertTrue(result.files().stream().allMatch(c -> c.status() == ChangeStatus.UNMODIFIED));
            assertEquals(firstChangedAt, loader.getStatus().getLastChangedAt());
        }
    }

    // -------------------------------------------------------------------------
    // LoadStatus
    // -------------------------------------------------------------------------

    @Test
    void status_lastCheckedAt_setAfterLoad() throws Exception {
        try (FileLoader loader = new FileLoader(new SwitchableFileSource("python_hr_v1"))) {
            assertNull(loader.getStatus().getLastCheckedAt());
            loader.load();
            assertNotNull(loader.getStatus().getLastCheckedAt());
        }
    }

    @Test
    void status_lastChangedAt_setWhenChanges() throws Exception {
        try (FileLoader loader = new FileLoader(new SwitchableFileSource("python_hr_v1"))) {
            loader.load();
            // First load always files (all files are new)
            assertNotNull(loader.getStatus().getLastChangedAt());
        }
    }

    @Test
    void status_lastChangedAt_notUpdatedWhenUnchanged() throws Exception {
        try (FileLoader loader = new FileLoader(new SwitchableFileSource("python_hr_v1"))) {
            loader.load();
            Instant afterFirst = loader.getStatus().getLastChangedAt();
            loader.load(); // no files
            assertEquals(afterFirst, loader.getStatus().getLastChangedAt());
        }
    }

    @Test
    void status_loadedFiles_correct() throws Exception {
        SwitchableFileSource src = new SwitchableFileSource("python_hr_v1");
        try (FileLoader loader = new FileLoader(src)) {
            loader.load();
            assertTrue(loader.getStatus().getLoadedFiles().contains("file_only_in_v1.py"));
            assertFalse(loader.getStatus().getLoadedFiles().contains("file_only_in_v2.py"));

            src.switchTo("python_hr_v2");
            loader.load();
            assertFalse(loader.getStatus().getLoadedFiles().contains("file_only_in_v1.py"));
            assertTrue(loader.getStatus().getLoadedFiles().contains("file_only_in_v2.py"));
        }
    }

    @Test
    void status_errorTracking_setOnListFilesFailure() throws Exception {
        try (FileLoader loader = new FileLoader(new FailingSource())) {
            LoadResult result = loader.load();
            assertFalse(result.success());
            assertNotNull(result.error());
            FileLoaderStatus status = loader.getStatus();
            assertNotNull(status.getLastErrorAt());
            assertNotNull(status.getLastError());
            assertNotNull(status.getFirstErrorAt());
        }
    }

    // -------------------------------------------------------------------------
    // onChanged callback
    // -------------------------------------------------------------------------

    @Test
    void onChanged_callback_firedWhenFilesChange() throws Exception {
        AtomicInteger count = new AtomicInteger();
        SwitchableFileSource src = new SwitchableFileSource("python_hr_v1");
        FileLoader.ReloadCallback callback = (result) -> {
            if (result.success()) count.incrementAndGet();
        };
        try (FileLoader loader = new FileLoader(src, callback)) {
            loader.load();
            assertEquals(1, count.get());
            src.switchTo("python_hr_v2");
            loader.load();
            assertEquals(2, count.get());
        }
    }

    @Test
    void callback_reloaded_firedOnEverySuccess() throws Exception {
        AtomicInteger count = new AtomicInteger();
        SwitchableFileSource src = new SwitchableFileSource("python_hr_v1");

        FileLoader.ReloadCallback callback = (result) -> {
            if (result.success()) count.incrementAndGet();
        };

        try (FileLoader loader = new FileLoader(src, callback)) {
            loader.load();
            assertEquals(1, count.get());
            loader.load(); // no files — reloaded() still fires
            assertEquals(2, count.get());
        }
    }

    // -------------------------------------------------------------------------
    // Push-notification integration
    // -------------------------------------------------------------------------

    @Test
    void setChangeListener_triggersAutoReload() throws Exception {
        NotifyingSource src = new NotifyingSource("python_hr_v1");
        try (FileLoader loader = new FileLoader(src)) {
            loader.load();
            assertTrue(loader.getStatus().getLoadedFiles().contains("file_only_in_v1.py"));

            src.switchTo("python_hr_v2");
            src.triggerChange(); // invokes doReloadOnChange() → load() without explicit call

            assertFalse(loader.getStatus().getLoadedFiles().contains("file_only_in_v1.py"));
            assertTrue(loader.getStatus().getLoadedFiles().contains("file_only_in_v2.py"));
        }
    }
}
