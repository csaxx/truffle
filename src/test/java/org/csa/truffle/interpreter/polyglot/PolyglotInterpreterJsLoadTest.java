package org.csa.truffle.interpreter.polyglot;

import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import static org.junit.jupiter.api.Assertions.*;

class PolyglotInterpreterJsLoadTest {

    private static PolyglotInterpreter build(Map<String, String> files) throws Exception {
        PolyglotInterpreter interp = new PolyglotInterpreter();
        for (Map.Entry<String, String> e : files.entrySet()) {
            interp.addContext(TruffleLanguage.JS, e.getKey(), e.getValue());
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
        try (PolyglotInterpreter interp = build(Map.of("a.js", "var x = 1;"))) {
            assertTrue(interp.getContexts().contains("a.js"));
        }
    }

    @Test
    void multipleFiles_allLoaded() throws Exception {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("a.js", "var x = 1;");
        files.put("b.js", "var x = 2;");
        files.put("c.js", "var x = 3;");
        try (PolyglotInterpreter interp = build(files)) {
            assertEquals(List.of("a.js", "b.js", "c.js"), interp.getContexts());
        }
    }

    @Test
    void getMember_existingMember_returnsValue() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of("a.js", "var answer = 42;"))) {
            Value v = interp.getMember("a.js", "answer");
            assertNotNull(v);
            assertEquals(42, v.asInt());
        }
    }

    @Test
    void getMember_missingMember_throwsNoSuchElementException() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of("a.js", "var x = 1;"))) {
            assertThrows(NoSuchElementException.class, () -> interp.getMember("a.js", "nonexistent"));
        }
    }

    @Test
    void getMember_unknownFile_throwsNoSuchElementException() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of("a.js", "var x = 1;"))) {
            assertThrows(NoSuchElementException.class, () -> interp.getMember("no_such.js", "x"));
        }
    }

    @Test
    void getMembers_allContextsHaveMember_returnsAll() throws Exception {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("a.js", "function fn() { return 'a'; }");
        files.put("b.js", "function fn() { return 'b'; }");
        try (PolyglotInterpreter interp = build(files)) {
            assertEquals(2, interp.getMembers("fn").size());
        }
    }

    @Test
    void getMembers_missingMemberInContext_throwsNoSuchElementException() throws Exception {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("has.js",     "function fn() { return 'yes'; }");
        files.put("missing.js", "var other = 99;");
        try (PolyglotInterpreter interp = build(files)) {
            assertThrows(NoSuchElementException.class, () -> interp.getMembers("fn"));
        }
    }

    @Test
    void addContext_sameNameSameContent_isNoOp() throws Exception {
        try (PolyglotInterpreter interp = new PolyglotInterpreter()) {
            interp.addContext(TruffleLanguage.JS, "a.js", "var x = 1;");
            interp.addContext(TruffleLanguage.JS, "a.js", "var x = 1;");
            assertEquals(1, interp.getContexts().size());
            assertEquals(1, interp.getMember("a.js", "x").asInt());
        }
    }

    @Test
    void addContext_sameNameDifferentContent_reloadsContext() throws Exception {
        try (PolyglotInterpreter interp = new PolyglotInterpreter()) {
            interp.addContext(TruffleLanguage.JS, "a.js", "var x = 1;");
            interp.addContext(TruffleLanguage.JS, "a.js", "var y = 2;");
            assertEquals(1, interp.getContexts().size());
            assertFalse(interp.hasMember("a.js", "x"));
            assertEquals(2, interp.getMember("a.js", "y").asInt());
        }
    }

    @Test
    void removeContext_removesContext() throws Exception {
        PolyglotInterpreter interp = new PolyglotInterpreter();
        interp.addContext(TruffleLanguage.JS, "a.js", "var x = 1;");
        interp.removeContext("a.js");
        assertFalse(interp.hasContext("a.js"));
        assertTrue(interp.getContexts().isEmpty());
        assertThrows(NoSuchElementException.class, () -> interp.getMember("a.js", "x"));
        interp.close();
    }

    @Test
    void hasMember_presentMember_returnsTrue() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of("a.js", "var answer = 42;"))) {
            assertTrue(interp.hasMember("a.js", "answer"));
        }
    }

    @Test
    void hasMember_absentMember_returnsFalse() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of("a.js", "var x = 1;"))) {
            assertFalse(interp.hasMember("a.js", "nonexistent"));
        }
    }

    @Test
    void getMemberNames_returnsAllDefinedNames() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of("a.js", "var x = 1; var y = 2;"))) {
            var names = interp.getMemberNames("a.js");
            assertTrue(names.contains("x"));
            assertTrue(names.contains("y"));
        }
    }

    @Test
    void clear_clearsAllContexts() throws Exception {
        PolyglotInterpreter interp = new PolyglotInterpreter();
        interp.addContext(TruffleLanguage.JS, "a.js", "var x = 1;");
        interp.addContext(TruffleLanguage.JS, "b.js", "var y = 2;");
        interp.clear();
        assertTrue(interp.getContexts().isEmpty());
        // interpreter is reusable after reset
        interp.addContext(TruffleLanguage.JS, "c.js", "var z = 3;");
        assertEquals(List.of("c.js"), interp.getContexts());
        interp.close();
    }
}
