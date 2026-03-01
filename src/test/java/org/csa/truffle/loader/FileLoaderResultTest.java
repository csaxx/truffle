package org.csa.truffle.loader;

import org.csa.truffle.source.resource.ResourceSource;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class FileLoaderResultTest {

    // -------------------------------------------------------------------------
    // Success path — result fields
    // -------------------------------------------------------------------------

    @Test
    void loadResult_success_isTrue() throws Exception {
        try (FileLoader loader = new FileLoader(new ResourceSource("python_hr_v1"))) {
            LoadResult result = loader.load();
            assertTrue(result.success());
        }
    }

    @Test
    void loadResult_success_containsExpectedFiles() throws Exception {
        try (FileLoader loader = new FileLoader(new ResourceSource("python_hr_v1"))) {
            LoadResult result = loader.load();
            assertNotNull(result.changes());
            List<String> names = result.changes().stream().map(FileChangeInfo::filePath).toList();
            assertTrue(names.contains("file_only_in_v1.py"));
            assertTrue(names.contains("file_in_both_unchanged.py"));
            assertTrue(names.contains("file_in_both_changed.py"));
        }
    }

    @Test
    void loadResult_success_errorIsNull() throws Exception {
        try (FileLoader loader = new FileLoader(new ResourceSource("python_hr_v1"))) {
            LoadResult result = loader.load();
            assertNull(result.error());
        }
    }

    @Test
    void loadResult_success_statusHasLastCheckedAt() throws Exception {
        try (FileLoader loader = new FileLoader(new ResourceSource("python_hr_v1"))) {
            LoadResult result = loader.load();
            assertNotNull(result.status().getLastCheckedAt());
        }
    }

    // -------------------------------------------------------------------------
    // Success path — callback
    // -------------------------------------------------------------------------

    @Test
    void loadResult_reloadedCallback_receivesFileContents() throws Exception {
        AtomicReference<List<FileChangeInfo>> received = new AtomicReference<>();
        FileLoader.LoadCallback callback = (result) -> received.set(result.changes());
        try (FileLoader loader = new FileLoader(new ResourceSource("python_hr_v1"), callback)) {
            loader.load();
            assertNotNull(received.get());
            assertTrue(received.get().stream().anyMatch(c -> c.filePath().equals("file_only_in_v1.py")));
        }
    }

    @Test
    void loadResult_reloadedCallback_receivesStatus() throws Exception {
        AtomicReference<FileLoaderStatus> received = new AtomicReference<>();
        FileLoader.LoadCallback callback = (result) -> received.set(result.status());
        try (FileLoader loader = new FileLoader(new ResourceSource("python_hr_v1"), callback)) {
            LoadResult result = loader.load();
            assertSame(result.status(), received.get());
        }
    }

    // -------------------------------------------------------------------------
    // Failure path — result fields
    // -------------------------------------------------------------------------

    @Test
    void loadResult_error_isFalse() throws Exception {
        try (FileLoader loader = new FileLoader(new FileLoaderTest.FailingSource())) {
            LoadResult result = loader.load();
            assertFalse(result.success());
        }
    }

    @Test
    void loadResult_error_containsException() throws Exception {
        try (FileLoader loader = new FileLoader(new FileLoaderTest.FailingSource())) {
            LoadResult result = loader.load();
            assertNotNull(result.error());
            assertInstanceOf(IOException.class, result.error());
        }
    }

    @Test
    void loadResult_error_fileContentsIsNull() throws Exception {
        try (FileLoader loader = new FileLoader(new FileLoaderTest.FailingSource())) {
            LoadResult result = loader.load();
            assertNull(result.changes());
        }
    }

    // -------------------------------------------------------------------------
    // Failure path — callback
    // -------------------------------------------------------------------------

    @Test
    void loadResult_errorCallback_isCalled() throws Exception {
        AtomicBoolean errorCalled = new AtomicBoolean(false);
        AtomicReference<Exception> receivedEx = new AtomicReference<>();

        FileLoader.LoadCallback callback = (result) -> {
            if (!result.success()) errorCalled.set(true);
            receivedEx.set(result.error());
        };

        try (FileLoader loader = new FileLoader(new FileLoaderTest.FailingSource(), callback)) {
            loader.load();
            assertTrue(errorCalled.get());
            assertNotNull(receivedEx.get());
        }
    }

    @Test
    void loadResult_errorCallback_notFired_onSuccess() throws Exception {
        AtomicBoolean errorCalled = new AtomicBoolean(false);

        FileLoader.LoadCallback callback = (result) -> {
            if (!result.success()) errorCalled.set(true);
        };

        try (FileLoader loader = new FileLoader(new ResourceSource("python_hr_v1"), callback)) {
            loader.load();
            assertFalse(errorCalled.get());
        }
    }

    @Test
    void loadResult_reloadedCallback_notFired_onError() throws Exception {
        AtomicBoolean reloadedCalled = new AtomicBoolean(false);

        FileLoader.LoadCallback callback = (result) -> {
            if (result.success()) reloadedCalled.set(true);
        };
        try (FileLoader loader = new FileLoader(new FileLoaderTest.FailingSource(), callback)) {
            loader.load();
            assertFalse(reloadedCalled.get());
        }
    }
}
