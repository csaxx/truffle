package org.csa.truffle.interpreter.groovy;

import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

class GroovyInterpreterExecuteTest {

    static class TestCollector implements Collector<String> {
        final List<String> output = new ArrayList<>();
        @Override public void collect(String v) { output.add(v); }
        @Override public void close() {}
    }

    private static GroovyInterpreter build(Map<String, String> files) {
        GroovyInterpreter interp = new GroovyInterpreter();
        for (Map.Entry<String, String> e : files.entrySet()) {
            interp.addContext(e.getKey(), e.getValue());
        }
        return interp;
    }

    @Test
    void executeAll_invokesAllContexts() {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("a.groovy", "def fn(x, out) { out.collect('a:' + x) }");
        files.put("b.groovy", "def fn(x, out) { out.collect('b:' + x) }");
        try (GroovyInterpreter interp = build(files)) {
            TestCollector col = new TestCollector();
            interp.executeAll("fn", "hello", col);
            assertEquals(List.of("a:hello", "b:hello"), col.output);
        }
    }

    @Test
    void execute_singleFile_invokesOnlyThatFile() {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("a.groovy", "def fn(x, out) { out.collect('a:' + x) }");
        files.put("b.groovy", "def fn(x, out) { out.collect('b:' + x) }");
        try (GroovyInterpreter interp = build(files)) {
            TestCollector col = new TestCollector();
            interp.execute("b.groovy", "fn", "hello", col);
            assertEquals(List.of("b:hello"), col.output);
        }
    }

    @Test
    void executeAll_missingMemberInContext_throwsNoSuchElementException() {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("has.groovy",     "def fn(x, out) { out.collect('has:' + x) }");
        files.put("missing.groovy", "def other() { 99 }");   // no fn
        try (GroovyInterpreter interp = build(files)) {
            TestCollector col = new TestCollector();
            assertThrows(NoSuchElementException.class, () -> interp.executeAll("fn", "x", col));
        }
    }

    @Test
    void execute_missingMember_throwsNoSuchElementException() {
        try (GroovyInterpreter interp = build(Map.of("a.groovy", "def fn() { 1 }"))) {
            assertThrows(NoSuchElementException.class, () -> interp.execute("a.groovy", "nonexistent", "arg"));
        }
    }

    @Test
    void getMember_cachedOnSecondAccess() {
        try (GroovyInterpreter interp = build(Map.of("a.groovy", "def fn() { 7 }"))) {
            var v1 = interp.getMember("a.groovy", "fn");
            var v2 = interp.getMember("a.groovy", "fn");
            assertSame(v1, v2);  // same GroovyCallable instance returned from cache
        }
    }

    @Test
    void canExecute_function_returnsTrue() {
        try (GroovyInterpreter interp = build(Map.of("a.groovy", "def fn() { 42 }"))) {
            assertTrue(interp.canExecute("a.groovy", "fn"));
        }
    }

    @Test
    void canExecute_missingMember_throwsNoSuchElementException() {
        try (GroovyInterpreter interp = build(Map.of("a.groovy", "def fn() { 42 }"))) {
            assertThrows(NoSuchElementException.class, () -> interp.canExecute("a.groovy", "nonexistent"));
        }
    }

    @Test
    void executeVoid_invokesFunction() {
        try (GroovyInterpreter interp = build(Map.of("a.groovy", "def fn(x, out) { out.collect('v:' + x) }"))) {
            TestCollector col = new TestCollector();
            interp.executeVoid("a.groovy", "fn", "z", col);
            assertEquals(List.of("v:z"), col.output);
        }
    }

    @Test
    void executeAllVoid_invokesAllContexts() {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("a.groovy", "def fn(x, out) { out.collect('a:' + x) }");
        files.put("b.groovy", "def fn(x, out) { out.collect('b:' + x) }");
        try (GroovyInterpreter interp = build(files)) {
            TestCollector col = new TestCollector();
            interp.executeAllVoid("fn", "q", col);
            assertEquals(List.of("a:q", "b:q"), col.output);
        }
    }

    @Test
    void executeAllVoid_missingMemberInContext_throwsNoSuchElementException() {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("has.groovy",     "def fn(x, out) { out.collect('has:' + x) }");
        files.put("missing.groovy", "def other() { 99 }");
        try (GroovyInterpreter interp = build(files)) {
            TestCollector col = new TestCollector();
            assertThrows(NoSuchElementException.class, () -> interp.executeAllVoid("fn", "x", col));
        }
    }

    @Test
    void executeAllPresent_skipsContextsWithoutMember() {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("has.groovy",     "def fn(x, out) { out.collect('has:' + x) }");
        files.put("missing.groovy", "def other() { 99 }");  // no fn
        try (GroovyInterpreter interp = build(files)) {
            TestCollector col = new TestCollector();
            interp.executeAllVoidPresent("fn", "x", col);
            assertEquals(List.of("has:x"), col.output);
        }
    }
}
