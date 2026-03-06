package org.csa.truffle.interpreter.polyglot;

import org.apache.flink.util.Collector;
import org.graalvm.polyglot.PolyglotException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PolyglotContextConfigTest {

    static class TestCollector implements Collector<String> {
        final List<String> output = new ArrayList<>();
        @Override public void collect(String v) { output.add(v); }
        @Override public void close() {}
    }

    @Test
    void minimal_allowsHostObjectAccess() throws Exception {
        try (PolyglotInterpreter interp = new PolyglotInterpreter(PolyglotContextConfig.MINIMAL)) {
            interp.addContext(TruffleLanguage.PYTHON, "t.py",
                    "def fn(out): out.collect('ok')");
            TestCollector col = new TestCollector();
            interp.execute("t.py", "fn", col);
            assertEquals(List.of("ok"), col.output);
        }
    }

    @Test
    void minimal_deniesHostClassLookup() throws Exception {
        try (PolyglotInterpreter interp = new PolyglotInterpreter(PolyglotContextConfig.MINIMAL)) {
            interp.addContext(TruffleLanguage.JS, "t.js",
                    "function fn() { return Java.type('java.lang.String'); }");
            assertThrows(PolyglotException.class, () -> interp.execute("t.js", "fn"));
        }
    }

    @Test
    void full_allowsHostClassLookup() throws Exception {
        try (PolyglotInterpreter interp = new PolyglotInterpreter(PolyglotContextConfig.FULL)) {
            interp.addContext(TruffleLanguage.JS, "t.js",
                    "function fn() { return Java.type('java.lang.String'); }");
            // Should not throw — returns the String class
            assertNotNull(interp.execute("t.js", "fn"));
        }
    }

    @Test
    void sandboxed_deniesHostObjectAccess() throws Exception {
        try (PolyglotInterpreter interp = new PolyglotInterpreter(PolyglotContextConfig.SANDBOXED)) {
            interp.addContext(TruffleLanguage.JS, "t.js",
                    "function fn(col) { col.collect('x'); }");
            TestCollector col = new TestCollector();
            assertThrows(PolyglotException.class, () -> interp.execute("t.js", "fn", col));
        }
    }

    @Test
    void defaultConstructor_usesMINIMAL() throws Exception {
        try (PolyglotInterpreter interp = new PolyglotInterpreter()) {
            interp.addContext(TruffleLanguage.PYTHON, "t.py",
                    "def fn(out): out.collect('ok')");
            TestCollector col = new TestCollector();
            interp.execute("t.py", "fn", col);
            assertEquals(List.of("ok"), col.output);
        }
    }
}
