package org.csa.truffle.loader.source.s3;

import org.apache.commons.lang3.StringUtils;
import org.csa.truffle.loader.source.FileSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

/**
 * {@link FileSource} that loads  files from an S3-compatible object store.
 * Works with AWS S3 and MinIO. The caller constructs and configures the
 * {@link S3Client}; this class only performs GetObject calls.
 *
 * <p><b>AWS example:</b>
 * <pre>
 *   new S3Source(S3Client.create(), "my-bucket", "");
 * </pre>
 *
 * <p><b>MinIO example:</b>
 * <pre>
 *   S3Client s3 = S3Client.builder()
 *       .endpointOverride(URI.create("http://localhost:9000"))
 *       .region(Region.US_EAST_1)
 *       .credentialsProvider(StaticCredentialsProvider.create(
 *           AwsBasicCredentials.create("minioadmin", "minioadmin")))
 *       .forcePathStyle(true)
 *       .build();
 *   new S3Source(s3, "my-bucket", "");
 * </pre>
 */
public class S3Source implements FileSource {

    private static final Logger log = LoggerFactory.getLogger(S3Source.class);

    private final S3Client s3;
    private final String bucket;
    private final String prefix; // never ends with '/', may be empty

    public S3Source(S3Client s3, String bucket, String prefix) {
        this.s3 = s3;
        this.bucket = bucket;
        // Normalise: strip trailing slashes
        this.prefix = StringUtils.stripEnd(prefix, "/");
        log.info("Initialized: bucket={}, prefix={}",
                bucket, this.prefix.isEmpty() ? "(root)" : this.prefix);
    }

    @Override
    public LinkedHashMap<String, Optional<Instant>> listFiles() throws IOException {
        List<String> names = getObject("index.txt").lines()
                .map(String::trim)
                .filter(l -> !l.isEmpty() && !l.startsWith("#"))
                .toList();
        LinkedHashMap<String, Optional<Instant>> result = new LinkedHashMap<>();
        try {
            for (String name : names) {
                String key = prefix.isEmpty() ? name : prefix + "/" + name;
                Instant t = s3.headObject(r -> r.bucket(bucket).key(key)).lastModified();
                result.put(name, Optional.ofNullable(t));
            }
        } catch (S3Exception e) {
            throw new IOException("S3 error fetching metadata: " + e.getMessage(), e);
        }
        return result;
    }

    @Override
    public String readFile(String name) throws IOException {
        return getObject(name);
    }

    private String getObject(String name) throws IOException {
        String key = prefix.isEmpty() ? name : prefix + "/" + name;
        try {
            ResponseBytes<GetObjectResponse> resp = s3.getObjectAsBytes(
                    GetObjectRequest.builder().bucket(bucket).key(key).build());
            return resp.asUtf8String();
        } catch (S3Exception e) {
            throw new IOException(
                    "S3 error fetching s3://" + bucket + "/" + key + ": " + e.getMessage(), e);
        }
    }
}
