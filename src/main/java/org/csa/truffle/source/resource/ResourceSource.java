package org.csa.truffle.source.resource;

import org.apache.commons.io.IOUtils;
import org.csa.truffle.source.FileSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * {@link FileSource} that loads  files from classpath resources.
 * The directory must contain an {@code index.txt} listing filenames (one per line;
 * lines starting with {@code #} and blank lines are ignored).
 */
public class ResourceSource implements FileSource {

    private final String directory;

    public ResourceSource(String directory) {
        this.directory = directory;
    }

    @Override
    public Map<String, Optional<Instant>> listFiles() throws IOException {
        LinkedHashMap<String, Optional<Instant>> result = new LinkedHashMap<>();
        readResource(directory + "/index.txt").lines()
                .map(String::trim)
                .filter(l -> !l.isEmpty() && !l.startsWith("#"))
                .forEach(name -> result.put(name, Optional.empty()));
        return result;
    }

    @Override
    public String readFile(String name) throws IOException {
        return readResource(directory + "/" + name);
    }

    private String readResource(String path) throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            if (is == null) throw new IOException("Resource not found: " + path);
            return IOUtils.toString(is, StandardCharsets.UTF_8);
        }
    }
}
