package org.csa.truffle.source.git;

import org.csa.truffle.source.FileSourceConfig;

/**
 * @param repoUrl   e.g. {@code https://github.com/owner/repo}
 * @param directory directory inside the repo containing .py files
 * @param branch    e.g. {@code master}
 * @param token     PAT for private repos; {@code null} for public repos
 * @param forge     explicit forge type; {@code null} = auto-detect from URL
 * @param filemask  glob pattern matched against the filename; {@code null} means no filter
 */
public record GitSourceConfig(
        String repoUrl, String directory, String branch,
        String token, GitForgeType forge, String filemask
) implements FileSourceConfig {

    public GitSourceConfig(String repoUrl, String directory, String branch,
                           String token, GitForgeType forge) {
        this(repoUrl, directory, branch, token, forge, null);
    }

    @Override
    public FileSourceConfig withFilemask(String filemask) {
        return new GitSourceConfig(repoUrl, directory, branch, token, forge, filemask);
    }
}
