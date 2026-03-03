package org.csa.truffle.interpreter.polyglot;

import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import static org.junit.jupiter.api.Assertions.*;

class PolyglotInterpreterLoadTest {

    private static PolyglotInterpreter build(Map<String, String> files) throws Exception {
        PolyglotInterpreter interp = new PolyglotInterpreter();
        for (Map.Entry<String, String> e : files.entrySet()) {
            interp.addContext(TruffleLanguage.PYTHON, e.getKey(), e.getValue());
        }
        return interp;
    }

    @Test
    void emptyMap_noFilesLoaded() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of())) {
            assertTrue(interp.getContexts().isEmpty());
        }
    }

    @Test
    void singleFile_isLoaded() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of("a.py", "x = 1"))) {
            assertTrue(interp.getContexts().contains("a.py"));
        }
    }

    @Test
    void multipleFiles_allLoaded() throws Exception {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("a.py", "x = 1");
        files.put("b.py", "x = 2");
        files.put("c.py", "x = 3");
        try (PolyglotInterpreter interp = build(files)) {
            assertEquals(List.of("a.py", "b.py", "c.py"), interp.getContexts());
        }
    }

    @Test
    void getMember_existingMember_returnsValue() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of("a.py", "answer = 42"))) {
            Value v = interp.getMember("a.py", "answer");
            assertNotNull(v);
            assertEquals(42, v.asInt());
        }
    }

    @Test
    void getMember_missingMember_throwsNoSuchElementException() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of("a.py", "x = 1"))) {
            assertThrows(NoSuchElementException.class, () -> interp.getMember("a.py", "nonexistent"));
        }
    }

    @Test
    void getMember_unknownFile_throwsNoSuchElementException() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of("a.py", "x = 1"))) {
            assertThrows(NoSuchElementException.class, () -> interp.getMember("no_such.py", "x"));
        }
    }

    @Test
    void getMembers_allContextsHaveMember_returnsAll() throws Exception {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("a.py", "fn = lambda: 'a'");
        files.put("b.py", "fn = lambda: 'b'");
        try (PolyglotInterpreter interp = build(files)) {
            assertEquals(2, interp.getMembers("fn").size());
        }
    }

    @Test
    void getMembers_missingMemberInContext_throwsNoSuchElementException() throws Exception {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("has.py",     "fn = lambda: 'yes'");
        files.put("missing.py", "other = 99");
        try (PolyglotInterpreter interp = build(files)) {
            assertThrows(NoSuchElementException.class, () -> interp.getMembers("fn"));
        }
    }

    @Test
    void addContext_duplicateId_throwsIllegalArgumentException() throws Exception {
        try (PolyglotInterpreter interp = new PolyglotInterpreter()) {
            interp.addContext(TruffleLanguage.PYTHON, "a.py", "x = 1");
            assertThrows(IllegalArgumentException.class,
                    () -> interp.addContext(TruffleLanguage.PYTHON, "a.py", "x = 2"));
        }
    }

    @Test
    void hasMember_presentMember_returnsTrue() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of("a.py", "answer = 42"))) {
            assertTrue(interp.hasMember("a.py", "answer"));
        }
    }

    @Test
    void hasMember_absentMember_returnsFalse() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of("a.py", "x = 1"))) {
            assertFalse(interp.hasMember("a.py", "nonexistent"));
        }
    }

    @Test
    void getMemberNames_returnsAllDefinedNames() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of("a.py", "x = 1\ny = 2"))) {
            var names = interp.getMemberNames("a.py");
            assertTrue(names.contains("x"));
            assertTrue(names.contains("y"));
        }
    }

    @Test
    void reset_clearsAllContexts() throws Exception {
        PolyglotInterpreter interp = new PolyglotInterpreter();
        interp.addContext(TruffleLanguage.PYTHON, "a.py", "x = 1");
        interp.addContext(TruffleLanguage.PYTHON, "b.py", "y = 2");
        interp.reset();
        assertTrue(interp.getContexts().isEmpty());
        // interpreter is reusable after reset
        interp.addContext(TruffleLanguage.PYTHON, "c.py", "z = 3");
        assertEquals(List.of("c.py"), interp.getContexts());
        interp.close();
    }
}
