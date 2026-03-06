package org.csa.truffle.source.git;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.csa.truffle.source.FileSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.time.Instant;
import java.util.*;

/**
 * {@link FileSource} that auto-discovers files in a Git repository via the
 * forge's tree API — no clone required. Supports GitHub, GitLab, and Gitea /
 * Forgejo. File contents are fetched one at a time via raw-content HTTP URLs.
 *
 * <p>Each of the {@code filemasks} globs is matched against the filename; a file matches
 * if it matches any pattern. Pass {@code null} or empty array to include all files.
 * Each of the {@code excludeFilemasks} globs is matched against each path component;
 * a file is excluded if any pattern matches any component.
 * Discovered paths are sorted alphabetically.
 *
 * <p><b>GitHub example:</b>
 * <pre>
 *   new GitSource("https://github.com/owner/repo", "python", "main", "ghp_...");
 * </pre>
 *
 * <p><b>GitLab example:</b>
 * <pre>
 *   new GitSource("https://gitlab.com/group/repo", "python", "main", "glpat-...");
 * </pre>
 *
 * <p><b>Gitea / Forgejo example:</b>
 * <pre>
 *   new GitSource("https://gitea.example.com/user/repo", "python", "main", token, GitForgeType.GITEA);
 * </pre>
 *
 * <p>Pass {@code null} as the token for public repositories.
 *
 * <p><b>GitLab pagination note:</b> {@code listFiles()} fetches at most 100
 * items per page (one request). Repositories with more than 100 files under
 * the configured directory may not return all files.
 */
public class GitSource implements FileSource {

    private static final Logger log = LoggerFactory.getLogger(GitSource.class);

    private final HttpClient http;
    private final String rawBaseUrl;        // {provider-raw-prefix}/{branch}
    private final String apiBaseUrl;        // forge-specific REST API root
    private final String directory;
    private final String branch;
    private final GitForgeType gitForgeType;
    private final String token;             // nullable
    private final String[] filemasks;       // nullable
    private final String[] excludeFilemasks; // nullable

    public GitSource(GitSourceConfig config) {
        GitForgeType forge = config.forge() != null ? config.forge() : detectForge(config.repoUrl());
        this.directory = config.directory();
        this.token = config.token();
        this.filemasks = config.filemasks();
        this.excludeFilemasks = config.excludeFilemasks();
        this.branch = config.branch();
        this.gitForgeType = forge;
        this.rawBaseUrl = buildRawBase(config.repoUrl(), config.branch(), forge);
        this.apiBaseUrl = config.apiBaseUrl() != null
                ? config.apiBaseUrl()
                : buildApiBase(config.repoUrl(), config.branch(), forge);
        this.http = HttpClient.newHttpClient();
        log.info("Initialized: rawBaseUrl={}, apiBaseUrl={}, directory={}, auth={}, forge={}",
                rawBaseUrl, apiBaseUrl, directory,
                StringUtils.isNotBlank(token) ? "token" : "none", gitForgeType);
    }

    /**
     * Infers the forge type from the repo URL host.
     */
    public static GitForgeType detectForge(String repoUrl) {
        String host = URI.create(StringUtils.removeEnd(repoUrl, "/")).getHost();
        return "github.com".equals(host) ? GitForgeType.GITHUB : GitForgeType.GITLAB;
    }

    /**
     * Converts a repo URL + branch into the provider-specific raw-content base URL.
     */
    public static String buildRawBase(String repoUrl, String branch, GitForgeType gitForgeType) {
        String url = StringUtils.removeEnd(repoUrl, "/");
        URI uri = URI.create(url);
        int port = uri.getPort();
        String authority = port == -1 ? uri.getHost() : uri.getHost() + ":" + port;
        String base = uri.getScheme() + "://" + authority + uri.getPath();
        return switch (gitForgeType) {
            case GITHUB -> "https://raw.githubusercontent.com" + uri.getPath() + "/" + branch;
            case GITLAB -> base + "/-/raw/" + branch;
            case GITEA -> base + "/raw/branch/" + branch;
        };
    }

    /**
     * Returns the forge-specific REST API base URL for the given repository.
     * <ul>
     *   <li>GitHub  → {@code https://api.github.com/repos/{owner}/{repo}}</li>
     *   <li>GitLab  → {@code https://{host}/api/v4/projects/{url-encoded-path}}</li>
     *   <li>Gitea   → {@code https://{host}/api/v1/repos/{owner}/{repo}}</li>
     * </ul>
     */
    public static String buildApiBase(String repoUrl, String branch, GitForgeType forge) {
        String url = StringUtils.removeEnd(repoUrl, "/");
        URI uri = URI.create(url);
        int port = uri.getPort();
        String authority = port == -1 ? uri.getHost() : uri.getHost() + ":" + port;
        String path = uri.getPath(); // e.g., "/owner/repo"

        return switch (forge) {
            case GITHUB -> "https://api.github.com/repos" + path;
            case GITLAB -> {
                // GitLab project path uses %2F instead of / between namespace components
                String encodedPath = path.substring(1).replace("/", "%2F");
                yield uri.getScheme() + "://" + authority + "/api/v4/projects/" + encodedPath;
            }
            case GITEA -> uri.getScheme() + "://" + authority + "/api/v1/repos" + path;
        };
    }

    // -------------------------------------------------------------------------
    // listFiles
    // -------------------------------------------------------------------------

    @Override
    public Map<String, Optional<Instant>> listFiles() throws IOException {
        PathMatcher[] matchers = buildMatchers(filemasks);
        PathMatcher[] excludeMatchers = buildMatchers(excludeFilemasks);
        List<String> paths = switch (gitForgeType) {
            case GITHUB -> listFilesGitHub(matchers, excludeMatchers);
            case GITLAB -> listFilesGitLab(matchers, excludeMatchers);
            case GITEA -> listFilesGitea(matchers, excludeMatchers);
        };
        LinkedHashMap<String, Optional<Instant>> result = new LinkedHashMap<>();
        for (String p : paths) {
            result.put(p, Optional.empty());
        }
        return result;
    }

    private List<String> listFilesGitHub(PathMatcher[] matchers, PathMatcher[] excludeMatchers) throws IOException {
        String url = apiBaseUrl + "/git/trees/" + branch + "?recursive=1";
        String json = fetchApi(url);
        return parseGitHubTree(json, matchers, excludeMatchers);
    }

    private List<String> listFilesGitLab(PathMatcher[] matchers, PathMatcher[] excludeMatchers) throws IOException {
        String encodedDir = URLEncoder.encode(directory, StandardCharsets.UTF_8);
        String url = apiBaseUrl + "/repository/tree?path=" + encodedDir
                + "&recursive=true&ref=" + branch + "&per_page=100";
        String json = fetchApi(url);
        return parseGitLabTree(json, matchers, excludeMatchers);
    }

    private List<String> listFilesGitea(PathMatcher[] matchers, PathMatcher[] excludeMatchers) throws IOException {
        String url = apiBaseUrl + "/git/trees/" + branch + "?recursive=true";
        String json = fetchApi(url);
        return parseGitHubTree(json, matchers, excludeMatchers); // same JSON structure as GitHub
    }

    /**
     * Parses a GitHub/Gitea tree API response.
     * Expected: {@code { "tree": [ { "path": "...", "type": "blob"|"tree" } ], "truncated": bool }}
     */
    private List<String> parseGitHubTree(String json, PathMatcher[] matchers,
                                          PathMatcher[] excludeMatchers) throws IOException {
        JsonNode root = new ObjectMapper().readTree(json);
        if (root.path("truncated").asBoolean(false)) {
            log.warn("GitHub/Gitea tree response is truncated; some files under '{}' may be missing",
                    directory);
        }
        JsonNode tree = root.path("tree");
        String dirPrefix = directory.isEmpty() ? "" : directory + "/";
        List<String> paths = new ArrayList<>();
        for (JsonNode node : tree) {
            if (!"blob".equals(node.path("type").asText())) continue;
            String p = node.path("path").asText();
            if (!dirPrefix.isEmpty() && !p.startsWith(dirPrefix)) continue;
            String rel = dirPrefix.isEmpty() ? p : p.substring(dirPrefix.length());
            if (rel.isEmpty()) continue;
            if (matchesAnyExclude(rel, excludeMatchers)) continue;
            if (!matchesMasks(rel, matchers)) continue;
            paths.add(rel);
        }
        Collections.sort(paths);
        return paths;
    }

    /**
     * Parses a GitLab repository tree API response.
     * Expected: JSON array {@code [ { "path": "...", "type": "blob"|"tree" } ]}
     * Paths in the response are relative to the repo root, so the {@code directory}
     * prefix is stripped.
     */
    private List<String> parseGitLabTree(String json, PathMatcher[] matchers,
                                          PathMatcher[] excludeMatchers) throws IOException {
        JsonNode root = new ObjectMapper().readTree(json);
        String dirPrefix = directory.isEmpty() ? "" : directory + "/";
        List<String> paths = new ArrayList<>();
        for (JsonNode node : root) {
            if (!"blob".equals(node.path("type").asText())) continue;
            String p = node.path("path").asText();
            if (!dirPrefix.isEmpty() && !p.startsWith(dirPrefix)) continue;
            String rel = dirPrefix.isEmpty() ? p : p.substring(dirPrefix.length());
            if (rel.isEmpty()) continue;
            if (matchesAnyExclude(rel, excludeMatchers)) continue;
            if (!matchesMasks(rel, matchers)) continue;
            paths.add(rel);
        }
        Collections.sort(paths);
        return paths;
    }

    // -------------------------------------------------------------------------
    // readFile
    // -------------------------------------------------------------------------

    @Override
    public String readFile(String name) throws IOException {
        return fetch(directory + "/" + name);
    }

    // -------------------------------------------------------------------------
    // HTTP helpers
    // -------------------------------------------------------------------------

    private String fetch(String relativePath) throws IOException {
        String url = rawBaseUrl + "/" + relativePath;
        return httpGet(url, false);
    }

    private String fetchApi(String url) throws IOException {
        return httpGet(url, true);
    }

    private String httpGet(String url, boolean acceptJson) throws IOException {
        log.debug("GET {}", url);
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET();
        if (acceptJson) {
            builder.header("Accept", "application/json");
        }
        if (StringUtils.isNotBlank(token)) {
            builder.header("Authorization", "Bearer " + token);
        }
        HttpResponse<String> response;
        try {
            response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("HTTP request interrupted: " + url, e);
        }
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("HTTP " + response.statusCode() + " fetching: " + url);
        }
        return response.body();
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
