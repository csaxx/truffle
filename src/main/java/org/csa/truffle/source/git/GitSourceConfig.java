package org.csa.truffle.source.git;

import org.csa.truffle.source.FileSourceConfig;

/**
 * @param repoUrl   e.g. {@code https://github.com/owner/repo}
 * @param directory directory inside the repo containing .py files
 * @param branch    e.g. {@code master}
 * @param token     PAT for private repos; {@code null} for public repos
 * @param forge     explicit forge type; {@code null} = auto-detect from URL
 * @param filemasks glob patterns matched against the filename; {@code null} or empty means no filter
 */
public record GitSourceConfig(
        String repoUrl, String directory, String branch,
        String token, GitForgeType forge, String[] filemasks
) implements FileSourceConfig {

    public GitSourceConfig(String repoUrl, String directory, String branch,
                           String token, GitForgeType forge) {
        this(repoUrl, directory, branch, token, forge, null);
    }
}
