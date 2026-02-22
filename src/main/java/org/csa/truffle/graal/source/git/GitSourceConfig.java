package org.csa.truffle.graal.source.git;

import org.csa.truffle.graal.source.PythonSourceConfig;

/**
 * @param repoUrl   e.g. {@code https://github.com/owner/repo}
 * @param directory directory inside the repo containing index.txt / .py files
 * @param branch    e.g. {@code master}
 * @param token     PAT for private repos; {@code null} for public repos
 * @param forge     explicit forge type; {@code null} = auto-detect from URL
 */
public record GitSourceConfig(
        String repoUrl, String directory, String branch,
        String token, GitForgeType forge
) implements PythonSourceConfig {
}
