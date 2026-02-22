package org.csa.truffle.graal.source;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GitPythonSourceUrlTest {

    @Test
    void github() {
        String base = GitPythonSource.buildRawBase(
                "https://github.com/owner/repo", "main", ForgeType.GITHUB);
        assertEquals("https://raw.githubusercontent.com/owner/repo/main", base);
    }

    @Test
    void gitlab_hosted() {
        String base = GitPythonSource.buildRawBase(
                "https://gitlab.com/group/repo", "main", ForgeType.GITLAB);
        assertEquals("https://gitlab.com/group/repo/-/raw/main", base);
    }

    @Test
    void gitlab_selfhosted() {
        String base = GitPythonSource.buildRawBase(
                "https://git.corp.example/group/repo", "develop", ForgeType.GITLAB);
        assertEquals("https://git.corp.example/group/repo/-/raw/develop", base);
    }

    @Test
    void gitea() {
        String base = GitPythonSource.buildRawBase(
                "https://gitea.example.com/user/repo", "main", ForgeType.GITEA);
        assertEquals("https://gitea.example.com/user/repo/raw/branch/main", base);
    }

    @Test
    void forgejo() {
        String base = GitPythonSource.buildRawBase(
                "https://codeberg.org/user/repo", "main", ForgeType.GITEA);
        assertEquals("https://codeberg.org/user/repo/raw/branch/main", base);
    }

    @Test
    void autoDetect_github() {
        String base = GitPythonSource.buildRawBase(
                "https://github.com/owner/repo", "main",
                GitPythonSource.detectForge("https://github.com/owner/repo"));
        assertEquals("https://raw.githubusercontent.com/owner/repo/main", base);
    }

    @Test
    void autoDetect_fallbackToGitlab() {
        String base = GitPythonSource.buildRawBase(
                "https://gitlab.mycompany.io/group/repo", "main",
                GitPythonSource.detectForge("https://gitlab.mycompany.io/group/repo"));
        assertEquals("https://gitlab.mycompany.io/group/repo/-/raw/main", base);
    }
}
