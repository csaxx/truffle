package org.csa.truffle.source;

import org.csa.truffle.source.file.FileSystemSource;
import org.csa.truffle.source.file.FileSystemSourceConfig;
import org.csa.truffle.source.git.GitSource;
import org.csa.truffle.source.git.GitSourceConfig;
import org.csa.truffle.source.resource.ResourceSource;
import org.csa.truffle.source.resource.ResourceSourceConfig;
import org.csa.truffle.source.s3.S3Source;
import org.csa.truffle.source.s3.S3SourceConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;
import java.nio.file.Path;

public final class FileSourceFactory {

    private FileSourceFactory() {
    }

    public static FileSource create(FileSourceConfig config) {
        return switch (config) {
            case ResourceSourceConfig c -> new ResourceSource(c.directory());

            case GitSourceConfig c -> c.forge() != null
                    ? new GitSource(c.repoUrl(), c.directory(), c.branch(), c.token(), c.forge())
                    : new GitSource(c.repoUrl(), c.directory(), c.branch(), c.token());

            case FileSystemSourceConfig c -> new FileSystemSource(Path.of(c.directory()), c.watch());

            case S3SourceConfig c -> {
                S3ClientBuilder b = S3Client.builder();
                if (c.region() != null)
                    b.region(Region.of(c.region()));
                if (c.endpointUrl() != null)
                    b.endpointOverride(URI.create(c.endpointUrl())).forcePathStyle(true);
                if (c.accessKeyId() != null && c.secretKey() != null)
                    b.credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(c.accessKeyId(), c.secretKey())));
                yield new S3Source(b.build(), c.bucket(), c.prefix());
            }
            default -> throw new IllegalArgumentException("Unknown SourceConfig type: " + config.getClass().getName());
        };
    }
}
