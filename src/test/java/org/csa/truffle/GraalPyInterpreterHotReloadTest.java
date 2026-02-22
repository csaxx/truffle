package org.csa.truffle;

import org.apache.flink.util.Collector;
import org.csa.truffle.graal.GraalPyInterpreter;
import org.csa.truffle.graal.source.ResourcePythonSource;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GraalPyInterpreterHotReloadTest {

    /**
     * Test collector that implements Flink's public {@link Collector} interface so that
     * GraalPy's host-access policy can find the {@code collect} method via the public interface.
     */
    static class TestCollector implements Collector<String> {
        final List<String> output = new ArrayList<>();
        @Override
        public void collect(String value) { output.add(value); }
        @Override
        public void close() {}
    }

/** Invokes all loaded {@code process_element} functions for one input string. */
    private static List<String> invokeAll(GraalPyInterpreter interp, String input) {
        TestCollector col = new TestCollector();
        interp.getMembers("process_element").forEach(fn -> fn.execute(input, col));
        return col.output;
    }

    // -------------------------------------------------------------------------
    // Case 1: file present in v1 but not v2 (removed on reload)
    // -------------------------------------------------------------------------

    @Test
    void case1_fileOnlyInV1_presentBeforeReload() throws Exception {
        try (GraalPyInterpreter interp = new GraalPyInterpreter(new ResourcePythonSource("python_hr_v1"))) {
            interp.reload();
            assertTrue(interp.getLoadedFileNames().contains("file_only_in_v1.py"));
        }
    }

    @Test
    void case1_fileOnlyInV1_absentAfterReload() throws Exception {
        SwitchablePythonSource src = new SwitchablePythonSource("python_hr_v1");
        try (GraalPyInterpreter interp = new GraalPyInterpreter(src)) {
            interp.reload();
            src.switchTo("python_hr_v2");
            interp.reload();
            assertFalse(interp.getLoadedFileNames().contains("file_only_in_v1.py"));
        }
    }

    @Test
    void case1_fileOnlyInV1_outputPresentBeforeReload() throws Exception {
        try (GraalPyInterpreter interp = new GraalPyInterpreter(new ResourcePythonSource("python_hr_v1"))) {
            interp.reload();
            assertTrue(invokeAll(interp, "x").contains("v1_only:x"));
        }
    }

    @Test
    void case1_fileOnlyInV1_outputAbsentAfterReload() throws Exception {
        SwitchablePythonSource src = new SwitchablePythonSource("python_hr_v1");
        try (GraalPyInterpreter interp = new GraalPyInterpreter(src)) {
            interp.reload();
            src.switchTo("python_hr_v2");
            interp.reload();
            assertFalse(invokeAll(interp, "x").contains("v1_only:x"));
        }
    }

    // -------------------------------------------------------------------------
    // Case 2: file absent in v1 but present in v2 (added on reload)
    // -------------------------------------------------------------------------

    @Test
    void case2_fileOnlyInV2_absentBeforeReload() throws Exception {
        try (GraalPyInterpreter interp = new GraalPyInterpreter(new ResourcePythonSource("python_hr_v1"))) {
            interp.reload();
            assertFalse(interp.getLoadedFileNames().contains("file_only_in_v2.py"));
        }
    }

    @Test
    void case2_fileOnlyInV2_presentAfterReload() throws Exception {
        SwitchablePythonSource src = new SwitchablePythonSource("python_hr_v1");
        try (GraalPyInterpreter interp = new GraalPyInterpreter(src)) {
            interp.reload();
            src.switchTo("python_hr_v2");
            interp.reload();
            assertTrue(interp.getLoadedFileNames().contains("file_only_in_v2.py"));
        }
    }

    @Test
    void case2_fileOnlyInV2_outputAbsentBeforeReload() throws Exception {
        try (GraalPyInterpreter interp = new GraalPyInterpreter(new ResourcePythonSource("python_hr_v1"))) {
            interp.reload();
            assertFalse(invokeAll(interp, "x").contains("v2_only:x"));
        }
    }

    @Test
    void case2_fileOnlyInV2_outputPresentAfterReload() throws Exception {
        SwitchablePythonSource src = new SwitchablePythonSource("python_hr_v1");
        try (GraalPyInterpreter interp = new GraalPyInterpreter(src)) {
            interp.reload();
            src.switchTo("python_hr_v2");
            interp.reload();
            assertTrue(invokeAll(interp, "x").contains("v2_only:x"));
        }
    }

    // -------------------------------------------------------------------------
    // Case 3: file present in both with identical content (context reused)
    // -------------------------------------------------------------------------

    @Test
    void case3_unchangedFile_presentInBoth() throws Exception {
        SwitchablePythonSource src = new SwitchablePythonSource("python_hr_v1");
        try (GraalPyInterpreter interp = new GraalPyInterpreter(src)) {
            interp.reload();
            assertTrue(interp.getLoadedFileNames().contains("file_in_both_unchanged.py"));
            src.switchTo("python_hr_v2");
            interp.reload();
            assertTrue(interp.getLoadedFileNames().contains("file_in_both_unchanged.py"));
        }
    }

    @Test
    void case3_unchangedFile_sameOutputInBoth() throws Exception {
        SwitchablePythonSource src = new SwitchablePythonSource("python_hr_v1");
        try (GraalPyInterpreter interp = new GraalPyInterpreter(src)) {
            interp.reload();
            assertTrue(invokeAll(interp, "x").contains("unchanged:x"));
            src.switchTo("python_hr_v2");
            interp.reload();
            assertTrue(invokeAll(interp, "x").contains("unchanged:x"));
        }
    }

    // -------------------------------------------------------------------------
    // Case 4: file present in both with different content (context replaced)
    // -------------------------------------------------------------------------

    @Test
    void case4_changedFile_presentInBoth() throws Exception {
        SwitchablePythonSource src = new SwitchablePythonSource("python_hr_v1");
        try (GraalPyInterpreter interp = new GraalPyInterpreter(src)) {
            interp.reload();
            assertTrue(interp.getLoadedFileNames().contains("file_in_both_changed.py"));
            src.switchTo("python_hr_v2");
            interp.reload();
            assertTrue(interp.getLoadedFileNames().contains("file_in_both_changed.py"));
        }
    }

    @Test
    void case4_changedFile_differentOutputAfterReload() throws Exception {
        SwitchablePythonSource src = new SwitchablePythonSource("python_hr_v1");
        try (GraalPyInterpreter interp = new GraalPyInterpreter(src)) {
            interp.reload();
            assertTrue(invokeAll(interp, "x").contains("changed_v1:x"));
            assertFalse(invokeAll(interp, "x").contains("changed_v2:x"));

            src.switchTo("python_hr_v2");
            interp.reload();
            assertFalse(invokeAll(interp, "x").contains("changed_v1:x"));
            assertTrue(invokeAll(interp, "x").contains("changed_v2:x"));
        }
    }

    // -------------------------------------------------------------------------
    // reload() return value
    // -------------------------------------------------------------------------

    @Test
    void reloadReturnsTrueWhenFilesChange() throws Exception {
        SwitchablePythonSource src = new SwitchablePythonSource("python_hr_v1");
        try (GraalPyInterpreter interp = new GraalPyInterpreter(src)) {
            interp.reload();
            src.switchTo("python_hr_v2");
            assertTrue(interp.reload());
        }
    }

    @Test
    void reloadReturnsFalseWhenUnchanged() throws Exception {
        SwitchablePythonSource src = new SwitchablePythonSource("python_hr_v1");
        try (GraalPyInterpreter interp = new GraalPyInterpreter(src)) {
            interp.reload();
            assertFalse(interp.reload());
        }
    }
}
