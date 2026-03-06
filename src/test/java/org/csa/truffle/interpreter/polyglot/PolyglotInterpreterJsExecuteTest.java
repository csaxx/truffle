package org.csa.truffle.interpreter.polyglot;

import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import static org.junit.jupiter.api.Assertions.*;

class PolyglotInterpreterJsExecuteTest {

    static class TestCollector implements Collector<String> {
        final List<String> output = new ArrayList<>();
        @Override public void collect(String v) { output.add(v); }
        @Override public void close() {}
    }

    private static PolyglotInterpreter build(Map<String, String> files) throws Exception {
        PolyglotInterpreter interp = new PolyglotInterpreter();
        for (Map.Entry<String, String> e : files.entrySet()) {
            interp.addContext(TruffleLanguage.JS, e.getKey(), e.getValue());
        }
        return interp;
    }

    @Test
    void executeAll_invokesAllPresentFunctions() throws Exception {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("a.js", "function fn(x, out) { out.collect('a:' + x); }");
        files.put("b.js", "function fn(x, out) { out.collect('b:' + x); }");
        try (PolyglotInterpreter interp = build(files)) {
            TestCollector col = new TestCollector();
            interp.executeAll("fn", "hello", col);
            assertEquals(List.of("a:hello", "b:hello"), col.output);
        }
    }

    @Test
    void execute_singleFile_invokesOnlyThatFile() throws Exception {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("a.js", "function fn(x, out) { out.collect('a:' + x); }");
        files.put("b.js", "function fn(x, out) { out.collect('b:' + x); }");
        try (PolyglotInterpreter interp = build(files)) {
            TestCollector col = new TestCollector();
            interp.execute("b.js", "fn", "hello", col);
            assertEquals(List.of("b:hello"), col.output);
        }
    }

    @Test
    void executeAll_missingMemberInContext_throwsNoSuchElementException() throws Exception {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("has.js",     "function fn(x, out) { out.collect('has:' + x); }");
        files.put("missing.js", "var other = 99;");   // no fn
        try (PolyglotInterpreter interp = build(files)) {
            TestCollector col = new TestCollector();
            assertThrows(NoSuchElementException.class, () -> interp.executeAll("fn", "x", col));
        }
    }

    @Test
    void execute_missingMember_throwsNoSuchElementException() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of("a.js", "var x = 1;"))) {
            assertThrows(NoSuchElementException.class, () -> interp.execute("a.js", "nonexistent", "arg"));
        }
    }

    @Test
    void getMember_cachedOnSecondAccess() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of("a.js", "var val = 7;"))) {
            var v1 = interp.getMember("a.js", "val");
            var v2 = interp.getMember("a.js", "val");
            assertSame(v1, v2);  // same Value instance returned from cache
        }
    }

    @Test
    void canExecute_function_returnsTrue() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of("a.js", "function fn() {}"))) {
            assertTrue(interp.canExecute("a.js", "fn"));
        }
    }

    @Test
    void canExecute_nonCallable_returnsFalse() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of("a.js", "var val = 42;"))) {
            assertFalse(interp.canExecute("a.js", "val"));
        }
    }

    @Test
    void executeVoid_invokesFunction() throws Exception {
        try (PolyglotInterpreter interp = build(Map.of("a.js", "function fn(x, out) { out.collect('v:' + x); }"))) {
            TestCollector col = new TestCollector();
            interp.executeVoid("a.js", "fn", "z", col);
            assertEquals(List.of("v:z"), col.output);
        }
    }

    @Test
    void executeAllVoid_invokesAllContexts() throws Exception {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("a.js", "function fn(x, out) { out.collect('a:' + x); }");
        files.put("b.js", "function fn(x, out) { out.collect('b:' + x); }");
        try (PolyglotInterpreter interp = build(files)) {
            TestCollector col = new TestCollector();
            interp.executeAllVoid("fn", "q", col);
            assertEquals(List.of("a:q", "b:q"), col.output);
        }
    }

    @Test
    void executeAllVoid_missingMemberInContext_throwsNoSuchElementException() throws Exception {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("has.js",     "function fn(x, out) { out.collect('has:' + x); }");
        files.put("missing.js", "var other = 99;");
        try (PolyglotInterpreter interp = build(files)) {
            TestCollector col = new TestCollector();
            assertThrows(NoSuchElementException.class, () -> interp.executeAllVoid("fn", "x", col));
        }
    }

    @Test
    void executeAllPresent_skipsContextsWithoutMember() throws Exception {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("has.js",     "function fn(x, out) { out.collect('has:' + x); }");
        files.put("missing.js", "var other = 99;");  // no fn
        try (PolyglotInterpreter interp = build(files)) {
            TestCollector col = new TestCollector();
            interp.executeAllVoidPresent("fn", "x", col);
            assertEquals(List.of("has:x"), col.output);
        }
    }
}
