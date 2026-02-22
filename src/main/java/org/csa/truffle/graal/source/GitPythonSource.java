package org.csa.truffle.graal.source;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * {@link PythonSource} that fetches Python files from a Git repository
 * without cloning. Supports GitHub (github.com), GitLab (gitlab.com or
 * self-hosted), and Gitea / Forgejo (self-hosted). Files are fetched one
 * at a time via raw-content HTTP URLs; only {@code index.txt} and the
 * files it lists are downloaded.
 *
 * <p><b>GitHub example:</b>
 * <pre>
 *   new GitPythonSource("https://github.com/owner/repo", "python", "main", "ghp_...");
 * </pre>
 *
 * <p><b>GitLab example:</b>
 * <pre>
 *   new GitPythonSource("https://gitlab.com/group/repo", "python", "main", "glpat-...");
 * </pre>
 *
 * <p><b>Gitea / Forgejo example:</b>
 * <pre>
 *   new GitPythonSource("https://gitea.example.com/user/repo", "python", "main", token, ForgeType.GITEA);
 * </pre>
 *
 * <p>Pass {@code null} as the token for public repositories.
 */
public class GitPythonSource implements PythonSource {

    private static final Logger log = LoggerFactory.getLogger(GitPythonSource.class);

    private final HttpClient http;
    private final String rawBaseUrl;   // {provider-raw-prefix}/{branch}
    private final String directory;
    private final String token;        // nullable

    /**
     * Creates a {@code GitPythonSource} with an explicit forge type.
     * Use this constructor when the host cannot be auto-detected (e.g. Gitea
     * or Forgejo instances, or GitLab instances that do not match the
     * auto-detection heuristic).
     */
    public GitPythonSource(String repoUrl, String directory, String branch,
                           String token, ForgeType forgeType) {
        this.directory = directory;
        this.token = token;
        this.rawBaseUrl = buildRawBase(repoUrl, branch, forgeType);
        this.http = HttpClient.newHttpClient();
        log.info("Initialized: rawBaseUrl={}, directory={}, auth={}, forge={}",
                 rawBaseUrl, directory,
                 StringUtils.isNotBlank(token) ? "token" : "none", forgeType);
    }

    /**
     * Creates a {@code GitPythonSource} with auto-detected forge type.
     * {@code github.com} is detected as {@link ForgeType#GITHUB}; all other
     * hosts fall back to {@link ForgeType#GITLAB}. Use the 5-arg constructor
     * to explicitly specify {@link ForgeType#GITEA} for Gitea / Forgejo hosts.
     */
    public GitPythonSource(String repoUrl, String directory, String branch, String token) {
        this(repoUrl, directory, branch, token, detectForge(repoUrl));
    }

    /** Infers the forge type from the repo URL host. */
    static ForgeType detectForge(String repoUrl) {
        String host = URI.create(StringUtils.removeEnd(repoUrl, "/")).getHost();
        return "github.com".equals(host) ? ForgeType.GITHUB : ForgeType.GITLAB;
    }

    /** Converts a repo URL + branch into the provider-specific raw-content base URL. */
    static String buildRawBase(String repoUrl, String branch, ForgeType forgeType) {
        String url = StringUtils.removeEnd(repoUrl, "/");
        URI uri = URI.create(url);
        String base = uri.getScheme() + "://" + uri.getHost() + uri.getPath();
        return switch (forgeType) {
            case GITHUB -> "https://raw.githubusercontent.com" + uri.getPath() + "/" + branch;
            case GITLAB -> base + "/-/raw/" + branch;
            case GITEA  -> base + "/raw/branch/" + branch;
        };
    }

    @Override
    public List<String> listFiles() throws IOException {
        return fetch(directory + "/index.txt").lines()
                .map(String::trim)
                .filter(l -> !l.isEmpty() && !l.startsWith("#"))
                .toList();
    }

    @Override
    public String readFile(String name) throws IOException {
        return fetch(directory + "/" + name);
    }

    private String fetch(String relativePath) throws IOException {
        String url = rawBaseUrl + "/" + relativePath;
        log.debug("GET {}", url);
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET();
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
}
