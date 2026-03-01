package org.csa.truffle.source.s3;

import org.apache.commons.lang3.StringUtils;
import org.csa.truffle.source.FileSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.IOException;
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
 * <p>Objects whose key ends with {@code /}, objects under any {@code venv/} subtree,
 * and objects whose filename does not match {@code filemask} are excluded.
 * Results are sorted alphabetically by relative key.
 *
 * <p><b>AWS example:</b>
 * <pre>
 *   new S3Source(S3Client.create(), "my-bucket", "", "*.py");
 * </pre>
 *
 * <p><b>MinIO example:</b>
 * <pre>
 *   new S3Source(s3, "my-bucket", "scripts", "*.py");
 * </pre>
 */
public class S3Source implements FileSource {

    private static final Logger log = LoggerFactory.getLogger(S3Source.class);

    private final S3Client s3;
    private final String bucket;
    private final String prefix;   // never ends with '/', may be empty
    private final String filemask; // nullable

    public S3Source(S3Client s3, String bucket, String prefix, String filemask) {
        this.s3 = s3;
        this.bucket = bucket;
        this.prefix = StringUtils.stripEnd(prefix, "/");
        this.filemask = filemask;
        log.info("Initialized: bucket={}, prefix={}, filemask={}",
                bucket, this.prefix.isEmpty() ? "(root)" : this.prefix, filemask);
    }

    public S3Source(S3Client s3, String bucket, String prefix) {
        this(s3, bucket, prefix, null);
    }

    @Override
    public Map<String, Optional<Instant>> listFiles() throws IOException {
        PathMatcher matcher = buildMatcher(filemask);
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
                    if (isVenvPath(Path.of(rel))) continue;
                    if (!matchesMask(rel, matcher)) continue;
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

    // -------------------------------------------------------------------------
    // Shared helpers
    // -------------------------------------------------------------------------

    static boolean isVenvPath(Path relativePath) {
        for (Path component : relativePath) {
            if ("venv".equals(component.toString())) return true;
        }
        return false;
    }

    static PathMatcher buildMatcher(String filemask) {
        if (filemask == null) return null;
        return FileSystems.getDefault().getPathMatcher("glob:" + filemask);
    }

    static boolean matchesMask(String relativePath, PathMatcher matcher) {
        if (matcher == null) return true;
        Path filename = Path.of(relativePath).getFileName();
        return filename != null && matcher.matches(filename);
    }
}
