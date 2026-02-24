package org.csa.truffle.graal;

import org.csa.truffle.loader.source.FileSource;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Manages the lifecycle of per-file GraalPy execution contexts.
 * <p>
 * The caller must invoke {@link #reload()} before using the interpreter.
 * All contexts share the static {@code SHARED_ENGINE} so compiled ASTs are
 * reused across contexts.
 */
public class GraalPyInterpreter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GraalPyInterpreter.class);

    /**
     * Shared compilation engine — never closed; caches compiled ASTs across contexts.
     */
    private static final Engine SHARED_ENGINE = Engine.newBuilder("python").build();

    /**
     * Maps filename to GraalPy context.
     */
    private final HashMap<String, GraalPyContext> fileContexts = new LinkedHashMap<>();

    private final FileSource source;

    private volatile long generation = 0;
    private volatile Optional<Instant> lastDataAge = Optional.empty();

    public GraalPyInterpreter(FileSource source) {
        this.source = source;
        source.setChangeListener(this::onSourceChanged);
        log.debug("Initialized with source: {}", source.getClass().getSimpleName());
    }

    public long getGeneration() {
        return generation;
    }

    private Context createContext() {
        return Context.newBuilder("python")
                .engine(SHARED_ENGINE)
                .allowAllAccess(true)
                .build();
    }

    /**
     * Returns one {@link Value} per file for the given member name,
     * in index.txt order.
     */
    public List<Value> getMembers(String memberName) {
        return fileContexts.values().stream()
                .map(fc -> fc.context().getBindings("python").getMember(memberName))
                .toList();
    }

    /**
     * Returns one (filename, Value) pair per file for the given member name,
     * in index.txt order. Fetching name and value in a single pass is safe
     * against concurrent reloads.
     */
    public Map<GraalPyContext, Value> getNamedMembers(String memberName) {
        return fileContexts.values().stream()
                .collect(Collectors.toMap(c -> c, c -> c.context().getBindings("python").getMember(memberName)));
    }

    /**
     * Returns the names of all currently loaded files, in index order.
     */
    public List<String> getLoadedFileNames() {
        return List.copyOf(fileContexts.keySet());
    }

    /**
     * Queries the injected {@link FileSource} and reconciles per-file contexts:
     * removed files get their Context closed, new/changed files get a fresh Context.
     *
     * @return {@code true} if any file was added, removed, or changed; {@code false} otherwise.
     */
    public synchronized boolean reload() throws IOException {
        Map<String, Optional<Instant>> fileList = source.listFiles();
        List<String> currentNames = new ArrayList<>(fileList.keySet());
        log.debug("Reload started; source lists {} file(s)", currentNames.size());
        lastDataAge = fileList.values().stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .max(Comparator.naturalOrder());

        // Read contents and compute hashes for all current files
        Map<String, String> currentContents = new LinkedHashMap<>();
        Map<String, String> currentHashes = new LinkedHashMap<>();
        for (String name : currentNames) {
            String code = source.readFile(name);
            currentContents.put(name, code);
            currentHashes.put(name, sha256(name, code));
        }

        boolean changed = false;

        // Close and remove contexts for files no longer in index
        List<String> removed = fileContexts.keySet().stream()
                .filter(name -> !currentHashes.containsKey(name))
                .toList();
        for (String name : removed) {
            fileContexts.remove(name).context().close();
            changed = true;
        }
        if (!removed.isEmpty()) {
            log.info("Removed {} Python file(s): {}", removed.size(), removed);
        }

        // Add or replace contexts for new/changed files
        for (String name : currentNames) {
            String newHash = currentHashes.get(name);
            GraalPyContext existing = fileContexts.get(name);
            if (existing == null || !existing.hash().equals(newHash)) {
                if (existing == null) {
                    log.info("Loading new Python file: {}", name);
                } else {
                    log.info("Reloading changed Python file: {}", name);
                    existing.context().close();
                }
                String code = currentContents.get(name);
                Context ctx = createContext();
                ctx.eval(Source.newBuilder("python", code, name).build());
                fileContexts.put(name, new GraalPyContext(ctx, name, newHash));
                changed = true;
            }
        }

        if (!changed) {
            log.debug("Reload complete: no changes detected");
            return false;
        }

        // Rebuild map in currentNames order (LinkedHashMap re-insertion)
        List<Map.Entry<String, GraalPyContext>> entries = currentNames.stream()
                .filter(fileContexts::containsKey)
                .map(name -> Map.entry(name, fileContexts.get(name)))
                .toList();
        fileContexts.clear();
        entries.forEach(e -> fileContexts.put(e.getKey(), e.getValue()));

        generation++;
        log.debug("Reload complete: generation advanced to {}", generation);
        return true;
    }

    /**
     * Returns the most recent modification timestamp across all files from the last
     * {@link #reload()} call, or {@link Optional#empty()} if the source does not
     * provide modification times.
     */
    public Optional<Instant> getDataAge() {
        return lastDataAge;
    }

    private void onSourceChanged() {
        log.info("Source change detected; triggering auto-reload");
        try {
            reload(); // generation counter drives staleness; result unused here
        } catch (IOException e) {
            log.error("Auto-reload failed", e);
        }
    }

    private static String sha256(String name, String content) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(name.getBytes(StandardCharsets.UTF_8));
            md.update((byte) 0); // separator avoids "a"+"bc" == "ab"+"c"
            md.update(content.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    @Override
    public void close() {
        log.debug("Closing interpreter: {} file context(s)", fileContexts.size());
        fileContexts.values().forEach(fc -> fc.context().close());
        try {
            source.close();
        } catch (IOException ignored) {
        }
    }
}
