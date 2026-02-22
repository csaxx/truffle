package org.csa.truffle.graal.source;

/**
 * Identifies the Git forge type so {@link GitPythonSource} can build
 * the correct raw-content URL. Forgejo uses the same URL format as Gitea.
 */
public enum GitForgeType {
    GITHUB,
    GITLAB,
    /**
     * Gitea and Forgejo (same raw-content URL scheme).
     */
    GITEA
}
