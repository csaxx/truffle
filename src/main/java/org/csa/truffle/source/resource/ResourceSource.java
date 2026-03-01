package org.csa.truffle.source.resource;

import org.apache.commons.io.IOUtils;
import org.csa.truffle.source.FileSource;

import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.jar.JarFile;
import java.util.stream.Stream;

/**
 * {@link FileSource} that auto-discovers files from a classpath directory.
 * All regular files under {@code directory} are returned in alphabetical order,
 * with {@code venv/} subtrees and files that do not match {@code filemask} excluded.
 */
public class ResourceSource implements FileSource {

    private final String directory;
    private final String filemask;

    public ResourceSource(String directory, String filemask) {
        this.directory = directory;
        this.filemask = filemask;
    }

    public ResourceSource(String directory) {
        this(directory, null);
    }

    @Override
    public Map<String, Optional<Instant>> listFiles() throws IOException {
        URL dirUrl = getClass().getClassLoader().getResource(directory);
        if (dirUrl == null) throw new IOException("Classpath directory not found: " + directory);
        PathMatcher matcher = buildMatcher(filemask);

        List<String> names;
        String protocol = dirUrl.getProtocol();

        if ("file".equals(protocol)) {
            Path dirPath;
            try {
                dirPath = Paths.get(dirUrl.toURI());
            } catch (Exception e) {
                throw new IOException("Cannot resolve classpath directory: " + directory, e);
            }
            try (Stream<Path> walk = Files.walk(dirPath)) {
                names = walk
                        .filter(Files::isRegularFile)
                        .filter(p -> !isVenvPath(dirPath.relativize(p)))
                        .filter(p -> matchesMask(dirPath.relativize(p).toString().replace('\\', '/'), matcher))
                        .map(p -> dirPath.relativize(p).toString().replace('\\', '/'))
                        .sorted()
                        .toList();
            }
        } else if ("jar".equals(protocol)) {
            JarURLConnection conn = (JarURLConnection) dirUrl.openConnection();
            conn.setUseCaches(false);
            String entryName = conn.getEntryName(); // e.g., "python"
            String entryPrefix = (entryName != null && !entryName.isEmpty())
                    ? entryName + "/" : "";
            String fp = entryPrefix;
            try (JarFile jf = conn.getJarFile()) {
                names = jf.stream()
                        .filter(e -> !e.isDirectory())
                        .filter(e -> fp.isEmpty() || e.getName().startsWith(fp))
                        .map(e -> fp.isEmpty() ? e.getName() : e.getName().substring(fp.length()))
                        .filter(rel -> !rel.isEmpty())
                        .filter(rel -> !isVenvPath(Path.of(rel)))
                        .filter(rel -> matchesMask(rel, matcher))
                        .sorted()
                        .collect(java.util.stream.Collectors.toList());
            }
        } else {
            throw new IOException(
                    "Unsupported URL protocol '" + protocol + "' for classpath directory: " + directory);
        }

        LinkedHashMap<String, Optional<Instant>> result = new LinkedHashMap<>();
        for (String name : names) {
            result.put(name, Optional.empty());
        }
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

    // -------------------------------------------------------------------------
    // Shared helpers
    // -------------------------------------------------------------------------

    static boolean isVenvPath(Path relativePath) {
        for (Path component : relativePath) {
            if ("venv".equals(component.toString())) return true;
        }
        return false;
    }

    static PathMatcher buildMatcher(String filemask) {
        if (filemask == null) return null;
        return FileSystems.getDefault().getPathMatcher("glob:" + filemask);
    }

    static boolean matchesMask(String relativePath, PathMatcher matcher) {
        if (matcher == null) return true;
        Path filename = Path.of(relativePath).getFileName();
        return filename != null && matcher.matches(filename);
    }
}
