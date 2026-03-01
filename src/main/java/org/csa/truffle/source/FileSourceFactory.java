package org.csa.truffle.source;

import org.csa.truffle.source.file.FileSystemSource;
import org.csa.truffle.source.file.FileSystemSourceConfig;
import org.csa.truffle.source.git.GitSource;
import org.csa.truffle.source.git.GitSourceConfig;
import org.csa.truffle.source.resource.ResourceSource;
import org.csa.truffle.source.resource.ResourceSourceConfig;
import org.csa.truffle.source.s3.S3Source;
import org.csa.truffle.source.s3.S3SourceConfig;

import java.nio.file.Path;

public final class FileSourceFactory {

    private FileSourceFactory() {
    }

    public static FileSource create(FileSourceConfig config) {

        return switch (config) {
            case ResourceSourceConfig c ->
                    new ResourceSource(c.directory(), c.filemasks());

            case GitSourceConfig c -> c.forge() != null
                    ? new GitSource(c.repoUrl(), c.directory(), c.branch(), c.token(), c.forge(), c.filemasks())
                    : new GitSource(c.repoUrl(), c.directory(), c.branch(), c.token(),
                            GitSource.detectForge(c.repoUrl()), c.filemasks());

            case FileSystemSourceConfig c ->
                    new FileSystemSource(Path.of(c.directory()), c.watch(), c.filemasks());

            case S3SourceConfig c -> new S3Source(c);

            default -> throw new IllegalArgumentException(
                    "Unknown SourceConfig type: " + config.getClass().getName());
        };
    }
}
