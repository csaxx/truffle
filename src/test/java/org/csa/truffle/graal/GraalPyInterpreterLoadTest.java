package org.csa.truffle.graal;

import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import static org.junit.jupiter.api.Assertions.*;

class GraalPyInterpreterLoadTest {

    @Test
    void emptyMap_noFilesLoaded() throws Exception {
        try (GraalPyInterpreter interp = new GraalPyInterpreter(Map.of())) {
            assertTrue(interp.getLoadedFileNames().isEmpty());
        }
    }

    @Test
    void singleFile_isLoaded() throws Exception {
        try (GraalPyInterpreter interp = new GraalPyInterpreter(Map.of("a.py", "x = 1"))) {
            assertTrue(interp.getLoadedFileNames().contains("a.py"));
        }
    }

    @Test
    void multipleFiles_allLoaded() throws Exception {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("a.py", "x = 1");
        files.put("b.py", "x = 2");
        files.put("c.py", "x = 3");
        try (GraalPyInterpreter interp = new GraalPyInterpreter(files)) {
            assertEquals(List.of("a.py", "b.py", "c.py"), interp.getLoadedFileNames());
        }
    }

    @Test
    void getMember_existingMember_returnsValue() throws Exception {
        try (GraalPyInterpreter interp = new GraalPyInterpreter(Map.of("a.py", "answer = 42"))) {
            Value v = interp.getMember("a.py", "answer");
            assertNotNull(v);
            assertEquals(42, v.asInt());
        }
    }

    @Test
    void getMember_missingMember_throwsNoSuchElementException() throws Exception {
        try (GraalPyInterpreter interp = new GraalPyInterpreter(Map.of("a.py", "x = 1"))) {
            assertThrows(NoSuchElementException.class, () -> interp.getMember("a.py", "nonexistent"));
        }
    }

    @Test
    void getMember_unknownFile_throwsNoSuchElementException() throws Exception {
        try (GraalPyInterpreter interp = new GraalPyInterpreter(Map.of("a.py", "x = 1"))) {
            assertThrows(NoSuchElementException.class, () -> interp.getMember("no_such.py", "x"));
        }
    }

    @Test
    void getMembers_returnsOnlyPresentMembers() throws Exception {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("has.py",     "fn = lambda: 'yes'");
        files.put("missing.py", "other = 99");
        try (GraalPyInterpreter interp = new GraalPyInterpreter(files)) {
            assertEquals(1, interp.getMembers("fn").size());
        }
    }
}
