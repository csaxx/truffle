package org.csa.truffle.source;

import org.csa.truffle.source.s3.S3Source;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class S3SourceTest {

    private static final String BUCKET = "my-bucket";

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Stubs {@code s3.getObjectAsBytes(GetObjectRequest)} to return {@code content}
     * when the request matches bucket + key.
     */
    @SuppressWarnings("unchecked")
    private static void stubGet(S3Client s3, String bucket, String key, String content) {
        ResponseBytes<GetObjectResponse> bytes = mock(ResponseBytes.class);
        when(bytes.asUtf8String()).thenReturn(content);
        when(s3.getObjectAsBytes(argThat((GetObjectRequest r) ->
                r != null && bucket.equals(r.bucket()) && key.equals(r.key()))))
                .thenReturn(bytes);
    }

    /**
     * Creates an S3Object with the given key and last-modified time.
     */
    private static S3Object s3obj(String key, Instant lastModified) {
        return S3Object.builder().key(key).lastModified(lastModified).build();
    }

    private static S3Object s3obj(String key) {
        return s3obj(key, Instant.parse("2024-01-01T00:00:00Z"));
    }

    /**
     * Stubs {@code s3.listObjectsV2Paginator} to return a single page with the given objects.
     */
    @SuppressWarnings("unchecked")
    private static void stubList(S3Client s3, S3Object... objects) {
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(Arrays.asList(objects))
                .build();
        List<ListObjectsV2Response> pages = List.of(response);

        ListObjectsV2Iterable iterable = mock(ListObjectsV2Iterable.class);
        when(iterable.iterator()).thenAnswer(inv -> pages.iterator());
        when(s3.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(iterable);
    }

    // -------------------------------------------------------------------------
    // listFiles
    // -------------------------------------------------------------------------

    @Test
    void listFiles_listsObjectsInPrefix() throws IOException {
        S3Client s3 = mock(S3Client.class);
        Instant t = Instant.parse("2024-01-01T00:00:00Z");
        stubList(s3,
                s3obj("scripts/transform.py", t),
                s3obj("scripts/filter.py", t));

        S3Source src = new S3Source(s3, BUCKET, "scripts");
        Map<String, Optional<Instant>> files = src.listFiles();

        assertEquals(2, files.size());
        assertTrue(files.containsKey("transform.py"));
        assertTrue(files.containsKey("filter.py"));
    }

    @Test
    void listFiles_returnsTimestampFromListing() throws IOException {
        S3Client s3 = mock(S3Client.class);
        Instant expected = Instant.parse("2024-06-15T12:00:00Z");
        stubList(s3, s3obj("scripts/transform.py", expected));

        S3Source src = new S3Source(s3, BUCKET, "scripts");
        Map<String, Optional<Instant>> files = src.listFiles();

        assertEquals(Optional.of(expected), files.get("transform.py"));
    }

    @Test
    void listFiles_returnsAlphabeticalOrder() throws IOException {
        S3Client s3 = mock(S3Client.class);
        // objects returned out of alphabetical order by S3
        stubList(s3, s3obj("p/z.py"), s3obj("p/a.py"));

        S3Source src = new S3Source(s3, BUCKET, "p");
        Map<String, Optional<Instant>> files = src.listFiles();

        assertEquals(List.of("a.py", "z.py"), List.copyOf(files.keySet()));
    }

    @Test
    void listFiles_emptyPrefix_usesKeyAsRelPath() throws IOException {
        S3Client s3 = mock(S3Client.class);
        stubList(s3, s3obj("transform.py"));

        S3Source src = new S3Source(s3, BUCKET, "");
        Map<String, Optional<Instant>> files = src.listFiles();

        assertTrue(files.containsKey("transform.py"));
        verify(s3).listObjectsV2Paginator(argThat((ListObjectsV2Request r) ->
                r != null && "".equals(r.prefix())));
    }

    @Test
    void listFiles_nonEmptyPrefix_stripsPrefix() throws IOException {
        S3Client s3 = mock(S3Client.class);
        stubList(s3, s3obj("scripts/transform.py"));

        S3Source src = new S3Source(s3, BUCKET, "scripts");
        Map<String, Optional<Instant>> files = src.listFiles();

        assertTrue(files.containsKey("transform.py"));
        assertFalse(files.containsKey("scripts/transform.py"));
    }

    @Test
    void listFiles_skipsDirectoryMarkers() throws IOException {
        S3Client s3 = mock(S3Client.class);
        // S3 sometimes includes explicit directory marker objects ending with '/'
        stubList(s3, s3obj("scripts/"), s3obj("scripts/real.py"));

        S3Source src = new S3Source(s3, BUCKET, "scripts");
        Map<String, Optional<Instant>> files = src.listFiles();

        assertFalse(files.containsKey(""));
        assertTrue(files.containsKey("real.py"));
    }

    @Test
    void listFiles_skipsVenvObjects() throws IOException {
        S3Client s3 = mock(S3Client.class);
        stubList(s3, s3obj("scripts/venv/lib.py"), s3obj("scripts/keep.py"));

        S3Source src = new S3Source(s3, BUCKET, "scripts");
        Map<String, Optional<Instant>> files = src.listFiles();

        assertFalse(files.keySet().stream().anyMatch(k -> k.contains("venv")));
        assertTrue(files.containsKey("keep.py"));
    }

    @Test
    void listFiles_filemaskFiltersObjects() throws IOException {
        S3Client s3 = mock(S3Client.class);
        stubList(s3, s3obj("scripts/transform.py"), s3obj("scripts/notes.txt"));

        S3Source src = new S3Source(s3, BUCKET, "scripts", "*.py");
        Map<String, Optional<Instant>> files = src.listFiles();

        assertTrue(files.containsKey("transform.py"));
        assertFalse(files.containsKey("notes.txt"));
    }

    @Test
    void listFiles_throwsIoExceptionOnS3Error() {
        S3Client s3 = mock(S3Client.class);
        when(s3.listObjectsV2Paginator(any(ListObjectsV2Request.class)))
                .thenThrow(S3Exception.builder().message("Access Denied").build());

        S3Source src = new S3Source(s3, BUCKET, "scripts");
        assertThrows(IOException.class, src::listFiles);
    }

    // -------------------------------------------------------------------------
    // readFile
    // -------------------------------------------------------------------------

    @Test
    void readFile_emptyPrefix_correctKey() throws IOException {
        S3Client s3 = mock(S3Client.class);
        stubGet(s3, BUCKET, "transform.py", "content");

        S3Source src = new S3Source(s3, BUCKET, "");
        src.readFile("transform.py");

        verify(s3).getObjectAsBytes(argThat((GetObjectRequest r) ->
                r != null && "transform.py".equals(r.key())));
    }

    @Test
    void readFile_nonEmptyPrefix_correctKey() throws IOException {
        S3Client s3 = mock(S3Client.class);
        stubGet(s3, BUCKET, "scripts/transform.py", "content");

        S3Source src = new S3Source(s3, BUCKET, "scripts");
        src.readFile("transform.py");

        verify(s3).getObjectAsBytes(argThat((GetObjectRequest r) ->
                r != null && "scripts/transform.py".equals(r.key())));
    }

    @Test
    void readFile_trailingSlashInPrefix_isNormalized() throws IOException {
        S3Client s3 = mock(S3Client.class);
        stubGet(s3, BUCKET, "scripts/transform.py", "content");

        S3Source src = new S3Source(s3, BUCKET, "scripts/"); // trailing slash
        src.readFile("transform.py");

        verify(s3).getObjectAsBytes(argThat((GetObjectRequest r) ->
                r != null && "scripts/transform.py".equals(r.key())));
    }

    @Test
    void readFile_returnsUtf8Content() throws IOException {
        S3Client s3 = mock(S3Client.class);
        stubGet(s3, BUCKET, "transform.py", "def process_element(line, out): pass\n");

        S3Source src = new S3Source(s3, BUCKET, "");
        String content = src.readFile("transform.py");
        assertEquals("def process_element(line, out): pass\n", content);
    }

    @Test
    void readFile_throwsIoExceptionOnS3Error() {
        S3Client s3 = mock(S3Client.class);
        when(s3.getObjectAsBytes(any(GetObjectRequest.class)))
                .thenThrow(S3Exception.builder().message("NoSuchKey").build());

        S3Source src = new S3Source(s3, BUCKET, "");
        assertThrows(IOException.class, () -> src.readFile("transform.py"));
    }
}
