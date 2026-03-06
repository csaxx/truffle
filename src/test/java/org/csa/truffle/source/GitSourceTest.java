package org.csa.truffle.source;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import org.csa.truffle.source.git.GitForgeType;
import org.csa.truffle.source.git.GitSource;
import org.csa.truffle.source.git.GitSourceConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

class GitSourceTest {

    @RegisterExtension
    static WireMockExtension wireMock = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort().bindAddress("127.0.0.1"))
            .build();

    private String base() {
        return "http://127.0.0.1:" + wireMock.getPort();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * GITLAB source: apiBaseUrl = http://127.0.0.1:{port}/api/v4/projects/owner%2Frepo
     * rawBaseUrl  = http://127.0.0.1:{port}/owner/repo/-/raw/main
     */
    private GitSource gitlabSource(String token) {
        String apiBase = base() + "/api/v4/projects/owner%2Frepo";
        return new GitSource(new GitSourceConfig(base() + "/owner/repo", "python", "main",
                token, GitForgeType.GITLAB, null, null, apiBase));
    }

    /**
     * GITEA source: apiBaseUrl = http://127.0.0.1:{port}/api/v1/repos/owner/repo
     * rawBaseUrl  = http://127.0.0.1:{port}/owner/repo/raw/branch/main
     */
    private GitSource giteaSource(String token) {
        String apiBase = base() + "/api/v1/repos/owner/repo";
        return new GitSource(new GitSourceConfig(base() + "/owner/repo", "python", "main",
                token, GitForgeType.GITEA, null, null, apiBase));
    }

    private void stubFile(String name, String body) {
        wireMock.stubFor(get(urlEqualTo("/owner/repo/-/raw/main/python/" + name))
                .willReturn(aResponse().withStatus(200).withBody(body)));
    }

    /** Stub the GitLab repository/tree endpoint. */
    private void stubGitLabTree(String body) {
        wireMock.stubFor(get(urlPathEqualTo("/api/v4/projects/owner%2Frepo/repository/tree"))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(body)));
    }

    /** Stub the Gitea git/trees endpoint. */
    private void stubGiteaTree(String branch, String body) {
        wireMock.stubFor(get(urlPathEqualTo("/api/v1/repos/owner/repo/git/trees/" + branch))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(body)));
    }

    // -------------------------------------------------------------------------
    // listFiles — GitLab
    // -------------------------------------------------------------------------

    @Test
    void listFiles_callsGitLabTreeApi() throws IOException {
        String json = """
                [
                  {"type":"blob","path":"python/transform.py"},
                  {"type":"blob","path":"python/filter.py"}
                ]""";
        stubGitLabTree(json);
        GitSource src = gitlabSource(null);
        Map<String, Optional<Instant>> files = src.listFiles();
        assertEquals(List.of("filter.py", "transform.py"), List.copyOf(files.keySet()));
        wireMock.verify(getRequestedFor(
                urlPathEqualTo("/api/v4/projects/owner%2Frepo/repository/tree")));
    }

    @Test
    void listFiles_allTimestampsAreEmpty() throws IOException {
        stubGitLabTree("""
                [{"type":"blob","path":"python/transform.py"}]""");
        GitSource src = gitlabSource(null);
        Map<String, Optional<Instant>> files = src.listFiles();
        for (Optional<Instant> ts : files.values()) {
            assertTrue(ts.isEmpty(), "expected empty timestamp for git source");
        }
    }

    @Test
    void listFiles_gitLab_skipsTreeEntries() throws IOException {
        // "tree" type entries (directories) should not be returned
        String json = """
                [
                  {"type":"tree","path":"python"},
                  {"type":"blob","path":"python/transform.py"}
                ]""";
        stubGitLabTree(json);
        GitSource src = gitlabSource(null);
        Map<String, Optional<Instant>> files = src.listFiles();
        assertEquals(List.of("transform.py"), List.copyOf(files.keySet()));
    }

    // -------------------------------------------------------------------------
    // listFiles — Gitea (same JSON structure as GitHub)
    // -------------------------------------------------------------------------

    @Test
    void listFiles_callsGiteaTreeApi() throws IOException {
        String json = """
                {
                  "tree": [
                    {"type":"blob","path":"python/transform.py"},
                    {"type":"blob","path":"python/filter.py"}
                  ],
                  "truncated": false
                }""";
        stubGiteaTree("main", json);
        GitSource src = giteaSource(null);
        Map<String, Optional<Instant>> files = src.listFiles();
        assertEquals(List.of("filter.py", "transform.py"), List.copyOf(files.keySet()));
        wireMock.verify(getRequestedFor(
                urlPathEqualTo("/api/v1/repos/owner/repo/git/trees/main")));
    }

    @Test
    void listFiles_gitea_skipsTreeEntries() throws IOException {
        String json = """
                {
                  "tree": [
                    {"type":"tree","path":"python"},
                    {"type":"blob","path":"python/transform.py"}
                  ],
                  "truncated": false
                }""";
        stubGiteaTree("main", json);
        GitSource src = giteaSource(null);
        Map<String, Optional<Instant>> files = src.listFiles();
        assertEquals(List.of("transform.py"), List.copyOf(files.keySet()));
    }

    // -------------------------------------------------------------------------
    // readFile
    // -------------------------------------------------------------------------

    @Test
    void readFile_fetchesCorrectUrl() throws IOException {
        stubFile("transform.py", "def process_element(line, out): pass\n");
        GitSource src = gitlabSource(null);
        String content = src.readFile("transform.py");
        assertEquals("def process_element(line, out): pass\n", content);
        wireMock.verify(getRequestedFor(
                urlEqualTo("/owner/repo/-/raw/main/python/transform.py")));
    }

    @Test
    void readFile_sendsAuthHeader() throws IOException {
        stubFile("transform.py", "body");
        GitSource src = gitlabSource("my-secret-token");
        src.readFile("transform.py");
        wireMock.verify(getRequestedFor(urlEqualTo("/owner/repo/-/raw/main/python/transform.py"))
                .withHeader("Authorization", equalTo("Bearer my-secret-token")));
    }

    @Test
    void readFile_noAuthHeaderWhenTokenNull() throws IOException {
        stubFile("transform.py", "body");
        GitSource src = gitlabSource(null);
        src.readFile("transform.py");
        wireMock.verify(getRequestedFor(urlEqualTo("/owner/repo/-/raw/main/python/transform.py"))
                .withoutHeader("Authorization"));
    }

    // -------------------------------------------------------------------------
    // Error handling
    // -------------------------------------------------------------------------

    @Test
    void listFiles_throwsOn404() {
        wireMock.stubFor(get(urlPathEqualTo("/api/v4/projects/owner%2Frepo/repository/tree"))
                .willReturn(aResponse().withStatus(404)));
        GitSource src = gitlabSource(null);
        assertThrows(IOException.class, src::listFiles);
    }

    @Test
    void listFiles_throwsOn500() {
        wireMock.stubFor(get(urlPathEqualTo("/api/v4/projects/owner%2Frepo/repository/tree"))
                .willReturn(aResponse().withStatus(500)));
        GitSource src = gitlabSource(null);
        assertThrows(IOException.class, src::listFiles);
    }

    @Test
    void readFile_throwsOnHttpError() {
        wireMock.stubFor(get(urlEqualTo("/owner/repo/-/raw/main/python/transform.py"))
                .willReturn(aResponse().withStatus(403)));
        GitSource src = gitlabSource(null);
        assertThrows(IOException.class, () -> src.readFile("transform.py"));
    }
}
