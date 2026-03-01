package org.csa.truffle;

import org.csa.truffle.source.FileSource;
import org.csa.truffle.source.resource.ResourceSource;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Test helper that delegates to a swappable {@link ResourceSource}.
 * Single-threaded use only (no synchronization).
 */
public class SwitchableFileSource implements FileSource {
    private ResourceSource current;

    public SwitchableFileSource(String initial) { current = new ResourceSource(initial); }

    public void switchTo(String dir) { current = new ResourceSource(dir); }

    @Override public Map<String, Optional<Instant>> listFiles() throws IOException { return current.listFiles(); }
    @Override public String readFile(String name) throws IOException { return current.readFile(name); }
}
