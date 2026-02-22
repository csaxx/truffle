package org.csa.truffle;

import org.csa.truffle.graal.PythonSource;
import org.csa.truffle.graal.source.ResourcePythonSource;

import java.io.IOException;
import java.util.List;

/**
 * Test helper that delegates to a swappable {@link ResourcePythonSource}.
 * Single-threaded use only (no synchronization).
 */
public class SwitchablePythonSource implements PythonSource {
    private ResourcePythonSource current;

    public SwitchablePythonSource(String initial) { current = new ResourcePythonSource(initial); }

    public void switchTo(String dir) { current = new ResourcePythonSource(dir); }

    @Override public List<String> listFiles() throws IOException { return current.listFiles(); }
    @Override public String readFile(String name) throws IOException { return current.readFile(name); }
}
