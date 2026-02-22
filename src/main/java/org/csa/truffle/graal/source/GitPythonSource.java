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
 * without cloning. Supports GitHub (github.com) and GitLab (gitlab.com or
 * self-hosted). Files are fetched one at a time via raw-content HTTP URLs;
 * only {@code index.txt} and the files it lists are downloaded.
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
 * <p>Pass {@code null} as the token for public repositories.
 */
public class GitPythonSource implements PythonSource {

    private static final Logger log = LoggerFactory.getLogger(GitPythonSource.class);

    private final HttpClient http;
    private final String rawBaseUrl;   // {provider-raw-prefix}/{branch}
    private final String directory;
    private final String token;        // nullable

    public GitPythonSource(String repoUrl, String directory, String branch, String token) {
        this.directory = directory;
        this.token = token;
        this.rawBaseUrl = buildRawBase(repoUrl, branch);
        this.http = HttpClient.newHttpClient();
        log.info("Initialized: rawBaseUrl={}, directory={}, auth={}",
                 rawBaseUrl, directory, StringUtils.isNotBlank(token) ? "token" : "none");
    }

    /** Converts a repo URL + branch into the provider-specific raw-content base URL. */
    private static String buildRawBase(String repoUrl, String branch) {
        String url = StringUtils.removeEnd(repoUrl, "/");
        URI uri = URI.create(url);
        String host = uri.getHost();
        String path = uri.getPath(); // e.g. /owner/repo

        if ("github.com".equals(host)) {
            return "https://raw.githubusercontent.com" + path + "/" + branch;
        } else {
            // GitLab (hosted or self-hosted) and compatible forges
            return uri.getScheme() + "://" + host + path + "/-/raw/" + branch;
        }
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
