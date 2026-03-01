package org.csa.truffle.source;

import org.csa.truffle.source.git.GitForgeType;
import org.csa.truffle.source.git.GitSource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link GitSource#buildRawBase} and {@link GitSource#buildApiBase}.
 */
class GitSourceUrlTest {

    // -------------------------------------------------------------------------
    // buildRawBase
    // -------------------------------------------------------------------------

    @Test
    void buildRawBase_github() {
        String result = GitSource.buildRawBase(
                "https://github.com/owner/repo", "main", GitForgeType.GITHUB);
        assertEquals("https://raw.githubusercontent.com/owner/repo/main", result);
    }

    @Test
    void buildRawBase_gitlab() {
        String result = GitSource.buildRawBase(
                "https://gitlab.com/group/repo", "main", GitForgeType.GITLAB);
        assertEquals("https://gitlab.com/group/repo/-/raw/main", result);
    }

    @Test
    void buildRawBase_gitea() {
        String result = GitSource.buildRawBase(
                "https://gitea.example.com/user/repo", "main", GitForgeType.GITEA);
        assertEquals("https://gitea.example.com/user/repo/raw/branch/main", result);
    }

    @Test
    void buildRawBase_stripsTrailingSlash() {
        String result = GitSource.buildRawBase(
                "https://github.com/owner/repo/", "main", GitForgeType.GITHUB);
        assertEquals("https://raw.githubusercontent.com/owner/repo/main", result);
    }

    @Test
    void buildRawBase_preservesPort() {
        String result = GitSource.buildRawBase(
                "http://gitea.local:3000/user/repo", "dev", GitForgeType.GITEA);
        assertEquals("http://gitea.local:3000/user/repo/raw/branch/dev", result);
    }

    // -------------------------------------------------------------------------
    // buildApiBase
    // -------------------------------------------------------------------------

    @Test
    void buildApiBase_github() {
        String result = GitSource.buildApiBase(
                "https://github.com/owner/repo", "main", GitForgeType.GITHUB);
        assertEquals("https://api.github.com/repos/owner/repo", result);
    }

    @Test
    void buildApiBase_gitlab() {
        String result = GitSource.buildApiBase(
                "https://gitlab.com/group/repo", "main", GitForgeType.GITLAB);
        assertEquals("https://gitlab.com/api/v4/projects/group%2Frepo", result);
    }

    @Test
    void buildApiBase_gitlab_nestedNamespace() {
        String result = GitSource.buildApiBase(
                "https://gitlab.com/a/b/c", "main", GitForgeType.GITLAB);
        assertEquals("https://gitlab.com/api/v4/projects/a%2Fb%2Fc", result);
    }

    @Test
    void buildApiBase_gitea() {
        String result = GitSource.buildApiBase(
                "https://gitea.example.com/user/repo", "main", GitForgeType.GITEA);
        assertEquals("https://gitea.example.com/api/v1/repos/user/repo", result);
    }

    @Test
    void buildApiBase_gitea_preservesPort() {
        String result = GitSource.buildApiBase(
                "http://gitea.local:3000/user/repo", "main", GitForgeType.GITEA);
        assertEquals("http://gitea.local:3000/api/v1/repos/user/repo", result);
    }

    // -------------------------------------------------------------------------
    // detectForge
    // -------------------------------------------------------------------------

    @Test
    void detectForge_githubCom() {
        assertEquals(GitForgeType.GITHUB, GitSource.detectForge("https://github.com/a/b"));
    }

    @Test
    void detectForge_otherHostIsGitlab() {
        assertEquals(GitForgeType.GITLAB, GitSource.detectForge("https://gitlab.com/a/b"));
        assertEquals(GitForgeType.GITLAB, GitSource.detectForge("https://self-hosted.example.com/a/b"));
    }
}
