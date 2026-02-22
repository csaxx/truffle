package org.csa.truffle.graal.source;

/**
 * @param bucket      S3 bucket name
 * @param prefix      key prefix (empty for bucket root)
 * @param region      AWS region; {@code null} = SDK default
 * @param endpointUrl MinIO / custom endpoint; {@code null} = AWS S3
 * @param accessKeyId explicit credential; {@code null} = default credential chain
 * @param secretKey   explicit credential; {@code null} = default credential chain
 */
public record S3SourceConfig(
        String bucket, String prefix,
        String region, String endpointUrl,
        String accessKeyId, String secretKey
) implements PythonSourceConfig {

    /**
     * AWS S3 with the default credential chain.
     */
    public static S3SourceConfig forAws(String bucket, String prefix) {
        return new S3SourceConfig(bucket, prefix, null, null, null, null);
    }

    /**
     * MinIO / custom endpoint with explicit credentials.
     */
    public static S3SourceConfig forMinio(String endpoint, String bucket, String prefix,
                                          String accessKeyId, String secretKey) {
        return new S3SourceConfig(bucket, prefix, "us-east-1", endpoint, accessKeyId, secretKey);
    }
}
