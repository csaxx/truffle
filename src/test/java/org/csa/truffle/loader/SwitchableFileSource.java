package org.csa.truffle.loader;

import org.csa.truffle.source.FileSource;
import org.csa.truffle.source.resource.ResourceSource;
import org.csa.truffle.source.resource.ResourceSourceConfig;

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

    public SwitchableFileSource(String initial) { current = new ResourceSource(new ResourceSourceConfig(initial)); }

    public void switchTo(String dir) { current = new ResourceSource(new ResourceSourceConfig(dir)); }

    @Override public Map<String, Optional<Instant>> listFiles() throws IOException { return current.listFiles(); }
    @Override public String readFile(String name) throws IOException { return current.readFile(name); }
}
