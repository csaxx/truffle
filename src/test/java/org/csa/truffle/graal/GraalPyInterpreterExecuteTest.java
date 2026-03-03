package org.csa.truffle.graal;

import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class GraalPyInterpreterExecuteTest {

    static class TestCollector implements Collector<String> {
        final List<String> output = new ArrayList<>();
        @Override public void collect(String v) { output.add(v); }
        @Override public void close() {}
    }

    private static GraalPyInterpreter build(Map<String, String> files) throws Exception {
        GraalPyInterpreter interp = new GraalPyInterpreter();
        for (Map.Entry<String, String> e : files.entrySet()) {
            interp.addFile(TruffleLanguage.PYTHON, e.getKey(), e.getValue());
        }
        return interp;
    }

    @Test
    void executeAll_invokesAllPresentFunctions() throws Exception {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("a.py", "def fn(x, out): out.collect('a:' + x)");
        files.put("b.py", "def fn(x, out): out.collect('b:' + x)");
        try (GraalPyInterpreter interp = build(files)) {
            TestCollector col = new TestCollector();
            interp.executeAll("fn", "hello", col);
            assertEquals(List.of("a:hello", "b:hello"), col.output);
        }
    }

    @Test
    void execute_singleFile_invokesOnlyThatFile() throws Exception {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("a.py", "def fn(x, out): out.collect('a:' + x)");
        files.put("b.py", "def fn(x, out): out.collect('b:' + x)");
        try (GraalPyInterpreter interp = build(files)) {
            TestCollector col = new TestCollector();
            interp.execute("b.py", "fn", "hello", col);
            assertEquals(List.of("b:hello"), col.output);
        }
    }

    @Test
    void executeAll_skipsFilesWithoutMember() throws Exception {
        LinkedHashMap<String, String> files = new LinkedHashMap<>();
        files.put("has.py",     "def fn(x, out): out.collect('has:' + x)");
        files.put("missing.py", "other = 99");   // no fn
        try (GraalPyInterpreter interp = build(files)) {
            TestCollector col = new TestCollector();
            interp.executeAll("fn", "x", col);
            assertEquals(List.of("has:x"), col.output);
        }
    }

    @Test
    void execute_missingMember_isNoOp() throws Exception {
        try (GraalPyInterpreter interp = build(Map.of("a.py", "x = 1"))) {
            // should not throw
            interp.execute("a.py", "nonexistent", "arg");
        }
    }

    @Test
    void getMember_cachedOnSecondAccess() throws Exception {
        try (GraalPyInterpreter interp = build(Map.of("a.py", "val = 7"))) {
            var v1 = interp.getMember("a.py", "val");
            var v2 = interp.getMember("a.py", "val");
            assertSame(v1, v2);  // same Value instance returned from cache
        }
    }
}
