package org.csa.truffle.interpreter.groovy;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

class GroovyInterpreterLoadTest {

    private static GroovyInterpreter build(Map<String, String> files) {
        GroovyInterpreter interp = new GroovyInterpreter();
        for (Map.Entry<String, String> e : files.entrySet()) {
            interp.addContext(e.getKey(), e.getValue());
        }
        return interp;
    }

    @Test
    void emptyMap_noFilesLoaded() {
        try (GroovyInterpreter interp = build(Map.of())) {
            assertTrue(interp.getContexts().isEmpty());
        }
    }

    @Test
    void singleFile_isLoaded() {
        try (GroovyInterpreter interp = build(Map.of("a.groovy", "def fn() { 42 }"))) {
            assertTrue(interp.getContexts().contains("a.groovy"));
        }
    }

    @Test
    void multipleFiles_allLoaded() {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("a.groovy", "def fn() { 1 }");
        files.put("b.groovy", "def fn() { 2 }");
        files.put("c.groovy", "def fn() { 3 }");
        try (GroovyInterpreter interp = build(files)) {
            assertEquals(List.of("a.groovy", "b.groovy", "c.groovy"), interp.getContexts());
        }
    }

    @Test
    void getMember_existingMember_returnsCallable() {
        try (GroovyInterpreter interp = build(Map.of("a.groovy", "def fn() { 42 }"))) {
            GroovyCallable callable = interp.getMember("a.groovy", "fn");
            assertNotNull(callable);
            assertEquals(42, callable.call());
        }
    }

    @Test
    void getMember_missingMember_throwsNoSuchElementException() {
        try (GroovyInterpreter interp = build(Map.of("a.groovy", "def fn() { 1 }"))) {
            assertThrows(NoSuchElementException.class, () -> interp.getMember("a.groovy", "nonexistent"));
        }
    }

    @Test
    void getMember_unknownFile_throwsNoSuchElementException() {
        try (GroovyInterpreter interp = build(Map.of("a.groovy", "def fn() { 1 }"))) {
            assertThrows(NoSuchElementException.class, () -> interp.getMember("no_such.groovy", "fn"));
        }
    }

    @Test
    void getMembers_allContextsHaveMember_returnsAll() {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("a.groovy", "def fn() { 'a' }");
        files.put("b.groovy", "def fn() { 'b' }");
        try (GroovyInterpreter interp = build(files)) {
            assertEquals(2, interp.getMembers("fn").size());
        }
    }

    @Test
    void getMembers_missingMemberInContext_throwsNoSuchElementException() {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("has.groovy",     "def fn() { 'yes' }");
        files.put("missing.groovy", "def other() { 99 }");
        try (GroovyInterpreter interp = build(files)) {
            assertThrows(NoSuchElementException.class, () -> interp.getMembers("fn"));
        }
    }

    @Test
    void addContext_duplicateId_throwsIllegalArgumentException() {
        try (GroovyInterpreter interp = new GroovyInterpreter()) {
            interp.addContext("a.groovy", "def fn() { 1 }");
            assertThrows(IllegalArgumentException.class,
                    () -> interp.addContext("a.groovy", "def fn() { 2 }"));
        }
    }

    @Test
    void hasMember_presentMember_returnsTrue() {
        try (GroovyInterpreter interp = build(Map.of("a.groovy", "def fn() { 42 }"))) {
            assertTrue(interp.hasMember("a.groovy", "fn"));
        }
    }

    @Test
    void hasMember_absentMember_returnsFalse() {
        try (GroovyInterpreter interp = build(Map.of("a.groovy", "def fn() { 1 }"))) {
            assertFalse(interp.hasMember("a.groovy", "nonexistent"));
        }
    }

    @Test
    void getMemberNames_returnsAllDefinedNames() {
        try (GroovyInterpreter interp = build(Map.of("a.groovy", "def foo() { 1 }\ndef bar() { 2 }"))) {
            var names = interp.getMemberNames("a.groovy");
            assertTrue(names.contains("foo"));
            assertTrue(names.contains("bar"));
            assertFalse(names.contains("run"));
        }
    }

    @Test
    void reset_clearsAllContexts() {
        GroovyInterpreter interp = new GroovyInterpreter();
        interp.addContext("a.groovy", "def fn() { 1 }");
        interp.addContext("b.groovy", "def fn() { 2 }");
        interp.reset();
        assertTrue(interp.getContexts().isEmpty());
        // interpreter is reusable after reset
        interp.addContext("c.groovy", "def fn() { 3 }");
        assertEquals(List.of("c.groovy"), interp.getContexts());
        interp.close();
    }
}
