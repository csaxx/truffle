package org.csa.truffle.source.git;

import org.csa.truffle.source.FileSourceConfig;

/**
 * @param repoUrl          e.g. {@code https://github.com/owner/repo}
 * @param directory        directory inside the repo containing .py files
 * @param branch           e.g. {@code master}
 * @param token            PAT for private repos; {@code null} for public repos
 * @param forge            explicit forge type; {@code null} = auto-detect from URL
 * @param filemasks        glob patterns matched against the filename; {@code null} or empty means no filter
 * @param excludeFilemasks glob patterns matched against each path component; a file is excluded if any
 *                         pattern matches any component; {@code null} means no exclusions
 * @param apiBaseUrl       override the forge API base URL; {@code null} = auto-derive from {@code repoUrl}
 *                         (intended for test isolation only)
 */
public record GitSourceConfig(
        String repoUrl, String directory, String branch,
        String token, GitForgeType forge, String[] filemasks, String[] excludeFilemasks,
        String apiBaseUrl
) implements FileSourceConfig {

    public GitSourceConfig(String repoUrl, String directory, String branch,
                           String token, GitForgeType forge) {
        this(repoUrl, directory, branch, token, forge, null, null, null);
    }

    // -------------------------------------------------------------------------
    // Factory methods
    // -------------------------------------------------------------------------

    public static GitSourceConfig forGitHub(String repoUrl, String directory, String branch, String token) {
        return new GitSourceConfig(repoUrl, directory, branch, token, GitForgeType.GITHUB);
    }

    public static GitSourceConfig forGitHub(String repoUrl, String directory, String branch, String token,
                                            String[] filemasks, String[] excludeFilemasks) {
        return new GitSourceConfig(repoUrl, directory, branch, token, GitForgeType.GITHUB, filemasks, excludeFilemasks, null);
    }

    public static GitSourceConfig forGitLab(String repoUrl, String directory, String branch, String token) {
        return new GitSourceConfig(repoUrl, directory, branch, token, GitForgeType.GITLAB);
    }

    public static GitSourceConfig forGitLab(String repoUrl, String directory, String branch, String token,
                                            String[] filemasks, String[] excludeFilemasks) {
        return new GitSourceConfig(repoUrl, directory, branch, token, GitForgeType.GITLAB, filemasks, excludeFilemasks, null);
    }

    public static GitSourceConfig forGitea(String repoUrl, String directory, String branch, String token) {
        return new GitSourceConfig(repoUrl, directory, branch, token, GitForgeType.GITEA);
    }

    public static GitSourceConfig forGitea(String repoUrl, String directory, String branch, String token,
                                           String[] filemasks, String[] excludeFilemasks) {
        return new GitSourceConfig(repoUrl, directory, branch, token, GitForgeType.GITEA, filemasks, excludeFilemasks, null);
    }
}
