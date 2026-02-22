package org.csa.truffle.graal.source;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * {@link PythonSource} that loads Python files from classpath resources.
 * The directory must contain an {@code index.txt} listing filenames (one per line;
 * lines starting with {@code #} and blank lines are ignored).
 */
public class ResourcePythonSource implements PythonSource {

    private final String directory;

    public ResourcePythonSource(String directory) {
        this.directory = directory;
    }

    @Override
    public List<String> listFiles() throws IOException {
        return readResource(directory + "/index.txt").lines()
                .map(String::trim)
                .filter(l -> !l.isEmpty() && !l.startsWith("#"))
                .toList();
    }

    @Override
    public String readFile(String name) throws IOException {
        return readResource(directory + "/" + name);
    }

    private String readResource(String path) throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            if (is == null) throw new IOException("Resource not found: " + path);
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
