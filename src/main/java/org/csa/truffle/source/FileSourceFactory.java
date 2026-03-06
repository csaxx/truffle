package org.csa.truffle.source;

import org.csa.truffle.source.file.FileSystemSource;
import org.csa.truffle.source.file.FileSystemSourceConfig;
import org.csa.truffle.source.git.GitSource;
import org.csa.truffle.source.git.GitSourceConfig;
import org.csa.truffle.source.map.MapFileSource;
import org.csa.truffle.source.map.MapFileSourceConfig;
import org.csa.truffle.source.resource.ResourceSource;
import org.csa.truffle.source.resource.ResourceSourceConfig;
import org.csa.truffle.source.s3.S3Source;
import org.csa.truffle.source.s3.S3SourceConfig;

public final class FileSourceFactory {

    private FileSourceFactory() {
    }

    public static FileSource create(FileSourceConfig config) {

        return switch (config) {
            case ResourceSourceConfig c -> new ResourceSource(c);
            case GitSourceConfig c -> new GitSource(c);
            case FileSystemSourceConfig c -> new FileSystemSource(c);
            case S3SourceConfig c -> new S3Source(c);
            case MapFileSourceConfig c -> new MapFileSource(c);
            default -> throw new IllegalArgumentException(
                    "Unknown SourceConfig type: " + config.getClass().getName());
        };
    }
}
