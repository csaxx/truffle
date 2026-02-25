package org.csa.truffle.loader.source;

import org.csa.truffle.loader.source.git.GitForgeType;
import org.csa.truffle.loader.source.git.GitSource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GitSourceUrlTest {

    @Test
    void github() {
        String base = GitSource.buildRawBase(
                "https://github.com/owner/repo", "main", GitForgeType.GITHUB);
        assertEquals("https://raw.githubusercontent.com/owner/repo/main", base);
    }

    @Test
    void gitlab_hosted() {
        String base = GitSource.buildRawBase(
                "https://gitlab.com/group/repo", "main", GitForgeType.GITLAB);
        assertEquals("https://gitlab.com/group/repo/-/raw/main", base);
    }

    @Test
    void gitlab_selfhosted() {
        String base = GitSource.buildRawBase(
                "https://git.corp.example/group/repo", "develop", GitForgeType.GITLAB);
        assertEquals("https://git.corp.example/group/repo/-/raw/develop", base);
    }

    @Test
    void gitea() {
        String base = GitSource.buildRawBase(
                "https://gitea.example.com/user/repo", "main", GitForgeType.GITEA);
        assertEquals("https://gitea.example.com/user/repo/raw/branch/main", base);
    }

    @Test
    void forgejo() {
        String base = GitSource.buildRawBase(
                "https://codeberg.org/user/repo", "main", GitForgeType.GITEA);
        assertEquals("https://codeberg.org/user/repo/raw/branch/main", base);
    }

    @Test
    void autoDetect_github() {
        String base = GitSource.buildRawBase(
                "https://github.com/owner/repo", "main",
                GitSource.detectForge("https://github.com/owner/repo"));
        assertEquals("https://raw.githubusercontent.com/owner/repo/main", base);
    }

    @Test
    void autoDetect_fallbackToGitlab() {
        String base = GitSource.buildRawBase(
                "https://gitlab.mycompany.io/group/repo", "main",
                GitSource.detectForge("https://gitlab.mycompany.io/group/repo"));
        assertEquals("https://gitlab.mycompany.io/group/repo/-/raw/main", base);
    }
}
