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
 * with files matching any {@code excludeFilemasks} pattern (matched against each path component)
 * excluded, and files that do not match any of {@code filemasks} excluded.
 */
public class ResourceSource implements FileSource {

    private final String directory;
    private final String[] filemasks;
    private final String[] excludeFilemasks;

    public ResourceSource(String directory, String[] filemasks, String[] excludeFilemasks) {
        this.directory = directory;
        this.filemasks = filemasks;
        this.excludeFilemasks = excludeFilemasks;
    }

    public ResourceSource(String directory, String[] filemasks) {
        this(directory, filemasks, null);
    }

    public ResourceSource(String directory) {
        this(directory, null, null);
    }

    @Override
    public Map<String, Optional<Instant>> listFiles() throws IOException {
        URL dirUrl = getClass().getClassLoader().getResource(directory);
        if (dirUrl == null) throw new IOException("Classpath directory not found: " + directory);
        PathMatcher[] matchers = buildMatchers(filemasks);
        PathMatcher[] excludeMatchers = buildMatchers(excludeFilemasks);

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
                        .map(p -> dirPath.relativize(p).toString().replace('\\', '/'))
                        .filter(rel -> !matchesAnyExclude(rel, excludeMatchers))
                        .filter(rel -> matchesMasks(rel, matchers))
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
                        .filter(rel -> !matchesAnyExclude(rel, excludeMatchers))
                        .filter(rel -> matchesMasks(rel, matchers))
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

    static PathMatcher[] buildMatchers(String[] filemasks) {
        if (filemasks == null || filemasks.length == 0) return null;
        PathMatcher[] matchers = new PathMatcher[filemasks.length];
        for (int i = 0; i < filemasks.length; i++) {
            matchers[i] = FileSystems.getDefault().getPathMatcher("glob:" + filemasks[i]);
        }
        return matchers;
    }

    static boolean matchesMasks(String relativePath, PathMatcher[] matchers) {
        if (matchers == null) return true;
        Path filename = Path.of(relativePath).getFileName();
        if (filename == null) return false;
        for (PathMatcher matcher : matchers) {
            if (matcher.matches(filename)) return true;
        }
        return false;
    }

    /**
     * Returns {@code true} if any exclude pattern matches any component of {@code relativePath}.
     * This allows patterns like {@code "venv"} to exclude entire subtrees, and patterns like
     * {@code "flink_types.py"} to exclude a specific filename at any depth.
     */
    static boolean matchesAnyExclude(String relativePath, PathMatcher[] excludeMatchers) {
        if (excludeMatchers == null) return false;
        for (Path component : Path.of(relativePath)) {
            for (PathMatcher matcher : excludeMatchers) {
                if (matcher.matches(component)) return true;
            }
        }
        return false;
    }
}
