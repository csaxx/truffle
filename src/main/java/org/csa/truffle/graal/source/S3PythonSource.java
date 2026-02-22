package org.csa.truffle.graal.source;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.util.List;

/**
 * {@link PythonSource} that loads Python files from an S3-compatible object store.
 * Works with AWS S3 and MinIO. The caller constructs and configures the
 * {@link S3Client}; this class only performs GetObject calls.
 *
 * <p><b>AWS example:</b>
 * <pre>
 *   new S3PythonSource(S3Client.create(), "my-bucket", "python");
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
 *   new S3PythonSource(s3, "my-bucket", "python");
 * </pre>
 */
public class S3PythonSource implements PythonSource {

    private static final Logger log = LoggerFactory.getLogger(S3PythonSource.class);

    private final S3Client s3;
    private final String bucket;
    private final String prefix; // never ends with '/', may be empty

    public S3PythonSource(S3Client s3, String bucket, String prefix) {
        this.s3 = s3;
        this.bucket = bucket;
        // Normalise: strip trailing slashes
        this.prefix = StringUtils.stripEnd(prefix, "/");
        log.info("Initialized: bucket={}, prefix={}",
                 bucket, this.prefix.isEmpty() ? "(root)" : this.prefix);
    }

    @Override
    public List<String> listFiles() throws IOException {
        return getObject("index.txt").lines()
                .map(String::trim)
                .filter(l -> !l.isEmpty() && !l.startsWith("#"))
                .toList();
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
