package org.csa.truffle.source.s3;

import org.csa.truffle.source.FileSourceConfig;

/**
 * @param bucket      S3 bucket name
 * @param prefix      key prefix (empty for bucket root)
 * @param region      AWS region; {@code null} = SDK default
 * @param endpointUrl MinIO / custom endpoint; {@code null} = AWS S3
 * @param accessKeyId explicit credential; {@code null} = default credential chain
 * @param secretKey   explicit credential; {@code null} = default credential chain
 * @param filemask    glob pattern matched against the filename; {@code null} means no filter
 */
public record S3SourceConfig(
        String bucket, String prefix,
        String region, String endpointUrl,
        String accessKeyId, String secretKey,
        String filemask
) implements FileSourceConfig {

    /**
     * AWS S3 with the default credential chain.
     */
    public static S3SourceConfig forAws(String bucket, String prefix) {
        return new S3SourceConfig(bucket, prefix, null, null, null, null, null);
    }

    /**
     * MinIO / custom endpoint with explicit credentials.
     */
    public static S3SourceConfig forMinio(String endpoint, String bucket, String prefix,
                                          String accessKeyId, String secretKey) {
        return new S3SourceConfig(bucket, prefix, "us-east-1", endpoint, accessKeyId, secretKey, null);
    }

    @Override
    public FileSourceConfig withFilemask(String filemask) {
        return new S3SourceConfig(bucket, prefix, region, endpointUrl, accessKeyId, secretKey, filemask);
    }
}
