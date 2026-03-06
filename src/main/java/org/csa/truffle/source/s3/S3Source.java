package org.csa.truffle.source.s3;

import org.apache.commons.lang3.StringUtils;
import org.csa.truffle.source.FileSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * {@link FileSource} that auto-discovers files from an S3-compatible object store
 * by listing objects under the configured prefix. Works with AWS S3 and MinIO.
 *
 * <p>Objects whose key ends with {@code /}, objects matching any {@code excludeFilemasks}
 * pattern (matched against each path component), and objects whose filename does not match
 * any of {@code filemasks} are excluded.
 * Results are sorted alphabetically by relative key.
 *
 * <p><b>AWS example:</b>
 * <pre>
 *   new S3Source(S3Client.create(), "my-bucket", "", new String[]{"*.py"});
 * </pre>
 *
 * <p><b>MinIO example:</b>
 * <pre>
 *   new S3Source(s3, "my-bucket", "scripts", new String[]{"*.py"});
 * </pre>
 */
public class S3Source implements FileSource {

    private static final Logger log = LoggerFactory.getLogger(S3Source.class);

    private final S3Client s3;
    private final boolean ownsClient; // true when this instance built the client
    private final String bucket;
    private final String prefix;         // never ends with '/', may be empty
    private final String[] filemasks;    // nullable
    private final String[] excludeFilemasks; // nullable

    /**
     * Constructs an {@code S3Source} from a {@link S3SourceConfig}, building and
     * owning the {@link S3Client}. Optional config fields are applied only when set:
     * <ul>
     *   <li>{@code region} — overrides the SDK default region</li>
     *   <li>{@code endpointUrl} — redirects to a MinIO / custom endpoint (path-style forced)</li>
     *   <li>{@code accessKeyId} + {@code secretKey} — uses static credentials instead of
     *       the default credential chain</li>
     * </ul>
     * Call {@link #close()} (or use try-with-resources) to release the client.
     */
    public S3Source(S3SourceConfig config) {
        this(buildClient(config), true, config.bucket(), config.prefix(),
                config.filemasks(), config.excludeFilemasks());
    }

    /**
     * Constructs an {@code S3Source} with a pre-built {@link S3Client}.
     * The client is <em>not</em> closed when this instance is closed.
     * Intended for testing only (package-private).
     */
    S3Source(S3Client s3, S3SourceConfig config) {
        this(s3, false, config.bucket(), config.prefix(), config.filemasks(), config.excludeFilemasks());
    }

    private S3Source(S3Client s3, boolean ownsClient, String bucket, String prefix,
                     String[] filemasks, String[] excludeFilemasks) {
        this.s3 = s3;
        this.ownsClient = ownsClient;
        this.bucket = bucket;
        this.prefix = StringUtils.stripEnd(prefix, "/");
        this.filemasks = filemasks;
        this.excludeFilemasks = excludeFilemasks;
        log.info("Initialized: bucket={}, prefix={}, filemasks={}",
                bucket, this.prefix.isEmpty() ? "(root)" : this.prefix,
                filemasks != null ? java.util.Arrays.toString(filemasks) : "null");
    }

    private static S3Client buildClient(S3SourceConfig config) {
        S3ClientBuilder b = S3Client.builder();
        if (config.region() != null)
            b.region(Region.of(config.region()));
        if (config.endpointUrl() != null)
            b.endpointOverride(URI.create(config.endpointUrl())).forcePathStyle(true);
        if (config.accessKeyId() != null && config.secretKey() != null)
            b.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.accessKeyId(), config.secretKey())));
        return b.build();
    }

    @Override
    public Map<String, Optional<Instant>> listFiles() throws IOException {
        PathMatcher[] matchers = buildMatchers(filemasks);
        PathMatcher[] excludeMatchers = buildMatchers(excludeFilemasks);
        String searchPrefix = prefix.isEmpty() ? "" : prefix + "/";

        TreeMap<String, Optional<Instant>> sorted = new TreeMap<>();
        try {
            ListObjectsV2Iterable pages = s3.listObjectsV2Paginator(
                    ListObjectsV2Request.builder().bucket(bucket).prefix(searchPrefix).build());
            for (ListObjectsV2Response page : pages) {
                for (S3Object obj : page.contents()) {
                    if (obj.key().endsWith("/")) continue; // skip directory markers
                    String rel = obj.key().substring(searchPrefix.length());
                    if (rel.isEmpty()) continue;
                    if (matchesAnyExclude(rel, excludeMatchers)) continue;
                    if (!matchesMasks(rel, matchers)) continue;
                    sorted.put(rel, Optional.ofNullable(obj.lastModified()));
                }
            }
        } catch (S3Exception e) {
            throw new IOException("S3 error listing objects: " + e.getMessage(), e);
        }
        return new LinkedHashMap<>(sorted);
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

    /** Closes the {@link S3Client} when this instance built it via {@link #S3Source(S3SourceConfig)}. */
    @Override
    public void close() {
        if (ownsClient) s3.close();
    }

    // -------------------------------------------------------------------------
    // Shared helpers
    // -------------------------------------------------------------------------

    static PathMatcher[] buildMatchers(String[] filemasks) {
        if (filemasks == null || filemasks.length == 0) return null;
        PathMatcher[] matchers = new PathMatcher[filemasks.length];
        for (int i = 0; i < filemasks.length; i++) {
            matchers[i] = FileSystems.getDefault().getPathMatcher("glob:" + filemasks[i]);
        }
        return matchers;
    }

    static boolean matchesMasks(String relativePath, PathMatcher[] matchers) {
        if (matchers == null) return true;
        Path filename = Path.of(relativePath).getFileName();
        if (filename == null) return false;
        for (PathMatcher matcher : matchers) {
            if (matcher.matches(filename)) return true;
        }
        return false;
    }

    /**
     * Returns {@code true} if any exclude pattern matches any component of {@code relativePath}.
     */
    static boolean matchesAnyExclude(String relativePath, PathMatcher[] excludeMatchers) {
        if (excludeMatchers == null) return false;
        for (Path component : Path.of(relativePath)) {
            for (PathMatcher matcher : excludeMatchers) {
                if (matcher.matches(component)) return true;
            }
        }
        return false;
    }
}
