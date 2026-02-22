package org.csa.truffle.graal.source;

import org.csa.truffle.graal.source.file.FilePythonSource;
import org.csa.truffle.graal.source.file.FileSourceConfig;
import org.csa.truffle.graal.source.resource.GitPythonSource;
import org.csa.truffle.graal.source.resource.GitSourceConfig;
import org.csa.truffle.graal.source.resource.ResourcePythonSource;
import org.csa.truffle.graal.source.resource.ResourceSourceConfig;
import org.csa.truffle.graal.source.s3.S3PythonSource;
import org.csa.truffle.graal.source.s3.S3SourceConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;
import java.nio.file.Path;

public final class PythonSourceFactory {

    private PythonSourceFactory() {
    }

    public static PythonSource create(PythonSourceConfig config) {
        return switch (config) {
            case ResourceSourceConfig c -> new ResourcePythonSource(c.directory());

            case GitSourceConfig c -> c.forge() != null
                    ? new GitPythonSource(c.repoUrl(), c.directory(), c.branch(), c.token(), c.forge())
                    : new GitPythonSource(c.repoUrl(), c.directory(), c.branch(), c.token());

            case FileSourceConfig c -> new FilePythonSource(Path.of(c.directory()), c.watch());

            case S3SourceConfig c -> {
                S3ClientBuilder b = S3Client.builder();
                if (c.region() != null)
                    b.region(Region.of(c.region()));
                if (c.endpointUrl() != null)
                    b.endpointOverride(URI.create(c.endpointUrl())).forcePathStyle(true);
                if (c.accessKeyId() != null && c.secretKey() != null)
                    b.credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(c.accessKeyId(), c.secretKey())));
                yield new S3PythonSource(b.build(), c.bucket(), c.prefix());
            }
            default -> throw new IllegalArgumentException("Unknown PythonSourceConfig type: " + config.getClass().getName());
        };
    }
}
