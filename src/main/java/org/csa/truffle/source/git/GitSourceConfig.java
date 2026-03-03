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
 */
public record GitSourceConfig(
        String repoUrl, String directory, String branch,
        String token, GitForgeType forge, String[] filemasks, String[] excludeFilemasks
) implements FileSourceConfig {

    public GitSourceConfig(String repoUrl, String directory, String branch,
                           String token, GitForgeType forge) {
        this(repoUrl, directory, branch, token, forge, null, null);
    }

    public GitSourceConfig(String repoUrl, String directory, String branch,
                           String token, GitForgeType forge, String[] filemasks) {
        this(repoUrl, directory, branch, token, forge, filemasks, null);
    }
}
