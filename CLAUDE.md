# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Use `run_job.bat` instead of calling `mvn` directly. It sets `JAVA_HOME` and
the full Maven path automatically (both are needed on this machine).

```bat
rem Compile
run_job.bat compile

rem Run tests
run_job.bat test

rem Run the job (writes output/java/ and output/python/)
rem exec:exec spawns a child JVM so Flink 2.x classloader isolation works correctly.
run_job.bat exec

rem exec-git loads files from Git repo instead of resource dir.
run_job.bat exec-git

rem Build a fat JAR for cluster submission
run_job.bat package -DskipTests
```

### Environment notes (Windows)

- **`mvn` is not on PATH** — use `run_job.bat`, which delegates to `mvnw.cmd` (Maven Wrapper). On first run `mvnw.cmd` downloads Maven 3.9.12 into `~/.m2/wrapper/dists/` automatically.
- **JAVA_HOME must be set** to the GraalVM JDK before invoking Maven, otherwise
  the compiler reports "invalid target release: 21".
  Path: `E:\code\dist\jvm\graalvm-jdk-21.0.10+8.1`
- `run_job.bat` handles both of the above.

## IntelliJ Maven Runner

Four shared run configurations are committed under `.idea/runConfigurations/` and appear
automatically in the Run/Debug selector (`Maven compile`, `Maven test`, `Maven exec`,
`Maven exec-git`). They require a one-time per-machine setup in IntelliJ settings:

1. **Maven home path** — Settings → Build, Execution, Deployment → Build Tools → Maven →
   *Maven home path*:
   `C:\Users\csa\.m2\wrapper\dists\apache-maven-3.9.12`

2. **Runner JRE** — Settings → Build, Execution, Deployment → Build Tools → Maven →
   Runner → *JRE*: pick **GraalVM JDK 21**
   (`E:\code\dist\jvm\graalvm-jdk-21.0.10+8.1`).
   If it is not listed, add it first via File → Project Structure → SDKs.

No env-var overrides are needed in the XML files — the globally configured Runner JRE
provides GraalVM JDK 21 for both compilation and `exec:exec`'s `${java.home}` resolution.

`run_job.bat` remains available for terminal use and requires no setup.

## Architecture

This is an Apache Flink 2.x streaming job that reads bounded CSV data, transforms it, and writes results to disk.

**Flink version:** controlled by `<flink.version>` in `pom.xml` (currently `2.2.0`).

### Pipeline flow

```
classpath CSVs (data/sales_q{1,2,3}.csv)
  → TruffleJob#loadCsvLines()        // loads lines into List<String>
  → TruffleJob#runTransform(lines, fn)
      → env.fromData(lines)          // bounded DataStream<String>
      → .process(fn)                 // ProcessFunctionJava or ProcessFunctionPython
      → executeAndCollect()          // triggers execution, collects locally
  → TruffleJob#writeOutput()         // writes CSV header + rows
  → output/java/sales_transformed.csv    (ProcessFunctionJava)
  → output/python/sales_transformed.csv  (ProcessFunctionPython)
```

### Formatting style

This project uses the default JetBrains IntelliJ IDEA formatting style for Java code.

### Key design choices

- **No Flink sink:** results are collected via `DataStream.executeAndCollect()` and written to disk with plain Java I/O. This avoids the deprecated `SinkFunction` / `RichSinkFunction` API from Flink 1.x.
- **Parallelism 1:** set explicitly in `TruffleJob` to keep local output deterministic and single-file.
- **CSV resources are loaded before the Flink graph is built** — the driver reads them from the classpath into a `List<String>` and feeds the list to `env.fromData()`. All three quarterly files are concatenated; header rows are filtered inside `ProcessFunctionJava.processElement`.
- **`ProcessFunctionPython` delegates entirely to Python** — `processElement` iterates all loaded Python files and calls each file's `process_element(line, out)` in order. The Python function calls `out.collect(...)` directly via GraalPy polyglot interop; no return value is used. This means all output routing logic lives in Python, not Java.

### Project goal: Python-scriptable Flink ProcessFunctions

The primary goal is to allow Flink `ProcessFunction` logic to be written in Python and
executed inside a running Flink job via GraalVM's polyglot API (GraalPy). This enables
hot-reloading of transform logic without recompiling or redeploying the JAR.

**Correctness baseline:** `ProcessFunctionEquivalenceTest` runs the same 20-row
input through both the canonical Java implementation (`ProcessFunctionJava`) and the
Python implementation (`ProcessFunctionPython` → `transform.py`) and asserts their
outputs are identical row-by-row. This ensures Python parity with the Java reference.

### GraalPyInterpreter

`GraalPyInterpreter` (`src/main/java/org/csa/truffle/graal/GraalPyInterpreter.java`)
is a pure context manager — it manages the lifecycle of per-file GraalPy execution
contexts. File loading and change tracking are handled separately by `FileLoader`.
Construct it by passing a `Map<String, String>` of filename → Python source code.

**`GraalPyContext`** (`src/main/java/org/csa/truffle/graal/GraalPyContext.java`) is a
class that pairs a GraalPy `Context` with its filename and a `Map<String, Value>` member
cache. `getMember(String memberName)` returns the cached `Value` for the named Python
binding, or throws `NoSuchElementException` if the module does not define that name.
Results are cached on first access; a missing member triggers a polyglot lookup on each
call (absent members are the exception, not the norm).

**Per-file context isolation.** Each file gets its own `Context`. This prevents name
collisions — two files can both define `process_element` without one overwriting the
other. All contexts share a single static `Engine` (`SHARED_ENGINE`) so compiled ASTs
are cached across contexts; the per-file overhead is only interpreter state.

**Constructor.**
```java
new GraalPyInterpreter(Map<String, String> fileContents)
```
Iteration order of the map determines index order; pass a `LinkedHashMap` for
deterministic ordering. Each entry is evaluated into its own GraalPy `Context`
immediately at construction time.

**API.**
- `getLoadedFileNames()` — returns filenames in index order.
- `getMember(filename, memberName)` — returns the cached `Value` for the named member
  in the given file; throws `NoSuchElementException` if the file is not loaded or does
  not define that member.
- `getMembers(memberName)` — returns one `Value` per file that defines `memberName`,
  in index order; files that do not define the member are omitted.
- `execute(filename, memberName, args...)` — calls the named function in the given file;
  no-op if the file or member is absent.
- `executeAll(memberName, args...)` — calls the named function in every file that defines
  it, in index order.
- `close()` — closes all per-file `Context`s. Use try-with-resources.

**Adding a new Python transform file.** Create the `.py` file under
`src/main/resources/python/` with a `process_element(line, out)` function. No Java
changes are required and no `index.txt` entry is needed — files are auto-discovered by
`ResourceSource`. The function receives the raw CSV line as a string and the Flink
`Collector<String>`; call `out.collect(...)` to emit output rows.

### FileSource implementations

These classes (in `org.csa.truffle.source`) are used by `ProcessFunctionPython` via
`FileSourceFactory` to supply Python file contents to `FileLoader` and `ScheduledReloader`.

**`FileSource` interface** (`org.csa.truffle.source.FileSource`). `listFiles()` returns
an ordered `Map<String, Optional<Instant>>` of filename → modification timestamp;
`readFile(name)` returns file content. The optional timestamp is `empty()` if the source
cannot determine the mtime for a file. `setChangeListener(Runnable)` and `close()` have
default no-op implementations; push-capable sources override them.

All implementations **auto-discover** files by walking/listing their source — no
`index.txt` is needed. `venv/` subtrees are excluded at any nesting depth. The `filemasks`
`String[]` filters by filename — a file matches if its name matches **any** pattern;
`null` or empty array = no filter (accept all). Results are sorted alphabetically.
Filemasks are always supplied in the config constructor; no post-construction mutation exists.

Four implementations ship with the project, split into subpackages:

| Implementation (package) | Source | Config record |
|--------------------------|--------|---------------|
| `resource.ResourceSource` | Classpath JAR | `new ResourceSourceConfig("python")` |
| `git.GitSource` | GitHub / GitLab / Gitea via HTTP | `new GitSourceConfig(url, dir, branch, token, forge)` |
| `s3.S3Source` | AWS S3 / MinIO | `new S3SourceConfig(...)` or static helpers |
| `file.FileSystemSource` | Local filesystem + WatchService | `new FileSystemSourceConfig(path, watch)` |

**Config records and `FileSourceFactory`.** `FileSourceConfig` is a `Serializable` marker
interface with a single method `filemasks()` returning `String[]`. All record components are
primitives, Strings, or enums — serialization is guaranteed. `FileSourceFactory.create(FileSourceConfig)`
is a `final class` with a static factory method; it uses a `switch` on the concrete type
to instantiate the correct `FileSource`. S3 credential wiring and `Path` conversion live
here, keeping the source classes free of construction details. `ProcessFunctionPython` stores
a `FileSourceConfig`; `open()` creates a `ScheduledReloader` which calls the factory. The
default constructor uses `new ResourceSourceConfig("python", new String[]{"*.py"})`.

**`resource.ResourceSource`** auto-discovers files from the classpath by walking the
`{directory}` tree (handles both `file://` filesystem JARs and `jar://` running-from-JAR protocols).

**`git.GitSource`** auto-discovers files in a Git repository via the forge's tree API —
no clone required. Supports GitHub, GitLab, and Gitea/Forgejo. The forge is identified
by `GitForgeType` (GITHUB, GITLAB, GITEA); pass `null` in `GitSourceConfig` to
auto-detect from the URL (github.com → GITHUB, all others → GITLAB). Authentication:
set `token`; pass `null` for public repos. File contents are fetched via raw-content
HTTP. `GitSource` has a 7-arg public constructor accepting a pre-built `apiBaseUrl` for
test isolation.

**`s3.S3Source`** auto-discovers files via `listObjectsV2Paginator` (mtime comes free
from listing, no extra `HeadObject` calls). `S3SourceConfig` has two static helpers:
- `S3SourceConfig.forAws(bucket, prefix)` — AWS S3 with the default credential chain.
- `S3SourceConfig.forMinio(endpoint, bucket, prefix, accessKeyId, secretKey)` — MinIO /
  custom endpoint with explicit credentials.
An empty `prefix` fetches objects from the bucket root.
`FileSourceFactory` builds the `S3Client` from config fields.

**`file.FileSystemSource`** auto-discovers files from a local directory using
`Files.readString`. The `watch` boolean in `FileSystemSourceConfig` controls whether it
starts a `WatchService` daemon thread. When watching, `setChangeListener` monitors the
directory for `ENTRY_CREATE`, `ENTRY_MODIFY`, and `ENTRY_DELETE` events, debounces 100 ms,
then calls the registered callback. Call `close()` (or use try-with-resources) to stop
the watcher thread.

**Push-notification protocol** (`setChangeListener`). `FileSource` has a default no-op
`setChangeListener(Runnable onChanged)`. Sources that can detect changes (`FileSystemSource`)
override it to start a watcher; pull-only sources inherit the no-op default. `FileLoader`
registers `this::load` as the listener in its constructor.

**Adding a new `FileSource`.** Implement `listFiles()` and `readFile(name)` in a new
class (place it in an appropriate `source/` subpackage). Also create a config record
implementing `FileSourceConfig` (with `filemasks()` returning `String[]`), add a
corresponding `case` to `FileSourceFactory.create()`, and pass the config to
`new ProcessFunctionPython(yourConfig, schedulerConfig)`. No other changes needed.

### FileLoader

**`FileLoader`** (`src/main/java/org/csa/truffle/loader/FileLoader.java`) is a `Closeable`
content cache that reads `.py` files from a `FileSource` and tracks changes via per-file
modification timestamps.

**`load()`** (re)reads the file list from `source.listFiles()`. On the first call every file is
read; on subsequent calls a file is re-read only when its mtime has advanced (or the source cannot
provide a timestamp). Files no longer returned by `listFiles()` are evicted from the cache.
**Never throws** — I/O errors are captured and returned in the `LoadResult`. Updates
`FileLoaderStatus` on every call regardless of outcome.

**`LoadResult`** (`src/main/java/org/csa/truffle/loader/LoadResult.java`) is the return type of
`load()`. It is a record with four fields:
- `success` (`boolean`) — `true` when no I/O error occurred.
- `status` — the loader's `FileLoaderStatus` instance (same reference as `getStatus()`).
- `changes` (`List<FileChangeInfo>`) — per-file change details (ADDED / UNMODIFIED / MODIFIED /
  REMOVED); non-null on success, `null` on failure.
- `error` (`Exception`) — the caught exception; `null` on success.

**`FileChangeInfo`** (`src/main/java/org/csa/truffle/loader/FileChangeInfo.java`) is a record
with `filePath`, `modifiedAt` (`Optional<Instant>`), and `status` (`ChangeStatus` enum:
ADDED, UNMODIFIED, MODIFIED, REMOVED).

**`FileLoader.FileLoadCallback`** is a `@FunctionalInterface` inner interface fired after every
`load()` attempt with a single method: `onReload(LoadResult result)`.

**`getFileContents()`** returns a defensive `LinkedHashMap<String, String>` snapshot in index order.

**`getStatus()`** returns the `FileLoaderStatus` instance. Fields are package-private (written by
`FileLoader`) with public getters:
- `lastCheckedAt` — set on every `load()` call; `null` until the first call.
- `lastChangedAt` — set only when `load()` detects a change.
- `lastSuccessAt` — set on every successful `load()`.
- `lastDataAge` — max mtime across all listed files from the most recent load; `null` if none reported.
- `loadedFiles` — `Set<String>` snapshot of currently cached filenames.
- `lastErrorAt` / `lastError` / `firstErrorAt` — error streak tracking.

**Push-notification.** `FileLoader` calls `source.setChangeListener(this::load)` in its
constructor. When a push-capable source fires the listener, `load()` runs automatically.
Pull-only sources inherit the no-op default and are unaffected.

**`close()`** delegates to `source.close()`. Always use try-with-resources.

**Constructors:** `FileLoader(FileSource source)` and `FileLoader(FileSource source, FileLoader.FileLoadCallback callback)`.

**`FileLoaderTest`** (`src/test/java/org/csa/truffle/loader/FileLoaderTest.java`) covers
Cases 1–4 (removed / added / unchanged / changed files), `FileLoaderStatus` field tracking,
callback firing, and push-notification integration via `NotifyingSource` — an inner test helper
that captures the registered listener and exposes `triggerChange()` to fire it synchronously
without a real watcher. Uses `SwitchableFileSource` and the existing `python_hr_v1` /
`python_hr_v2` test resources.

**`FileLoaderResultTest`** (`src/test/java/org/csa/truffle/loader/FileLoaderResultTest.java`)
exercises `LoadResult` fields and `FileLoadCallback` dispatch in isolation using `ResourceSource`
and an inline `FailingSource`. Covers all combinations of success/failure × result fields ×
callback routing (12 tests).

### Scheduled reload

**`SchedulerConfig`** (`src/main/java/org/csa/truffle/scheduler/SchedulerConfig.java`)
is a `Serializable` record with two components: `Duration interval` and `Duration gracePeriod`.
Passing a config object (rather than a raw `Duration`) to `ProcessFunctionPython` mirrors the
`FileSourceConfig` pattern and ensures the value survives Flink serialization across distributed
operators. The single-arg constructor `SchedulerConfig(interval)` defaults `gracePeriod` to
`Duration.ZERO`, which means reload errors are tolerated indefinitely (logged but never fatal).
When `gracePeriod` is positive, background reload failures that persist longer than the grace
period cause `ScheduledReloader` to store a fatal `RuntimeException`; `ProcessFunctionPython`
checks for it at the top of every `processElement()` call and re-throws it, failing the Flink task.

**`ScheduledReloader`** (`src/main/java/org/csa/truffle/scheduler/ScheduledReloader.java`)
manages a single dataset backed by a `FileLoader` and drives periodic polling:

**Constructors:**
```java
new ScheduledReloader(FileSourceConfig sourceConfig, SchedulerConfig schedulerConfig, ScheduledReloadCallback callback)
new ScheduledReloader(FileSource source,            SchedulerConfig schedulerConfig, ScheduledReloadCallback callback)
```

**`ScheduledReloader.ScheduledReloadCallback`** is a `@FunctionalInterface` inner interface with
a single method:
```java
void onReload(FileLoaderStatus status, GraalPyInterpreter interpreter)
```
The callback is fired **only when changes are detected** (not on every successful poll), including
the initial synchronous load on `start()`. When the grace period is exceeded, the callback is
fired once more with `interpreter = null` to let the caller clean up.

**Lifecycle:**
- `start()` performs an initial **synchronous** reload on the calling thread (guaranteeing
  data is ready before Flink calls `processElement`), throws `IOException` if the initial load
  fails, then schedules background reloads via a single-daemon-thread `ScheduledExecutorService`
  at `schedulerConfig.interval()`.
- `checkForFatalError()` — throws the stored `fatalError` if the grace period was exceeded;
  no-op otherwise. Called by `ProcessFunctionPython.processElement()` on every record.
- `getStatus()` — delegates to `FileLoader.getStatus()` for observable reload timestamps.
- `getFatalError()` — returns the stored `RuntimeException` if the grace period was exceeded,
  or `null` otherwise.
- `getFirstErrorAt()` — returns the start of the current error streak (tracked inside
  `ScheduledReloader`, not `FileLoaderStatus`), or `null` if no errors have occurred.
- `close()` — calls `executor.shutdownNow()` and `loader.close()`. Use try-with-resources.

**Error streak tracking.** `firstErrorAt` is a `volatile Instant` owned by `ScheduledReloader`.
On each successful reload it is cleared to `null`. On the first failed reload it is set to
`Instant.now()`; subsequent failures compute `Duration.between(firstErrorAt, now)` and set
`fatalError` once the streak exceeds `gracePeriod`.

**`ProcessFunctionPython` constructors:**
- `()` — defaults to `new SchedulerConfig(Duration.ofMinutes(5))` + classpath `python/` dir.
- `(Duration interval)` — convenience overload; wraps in `SchedulerConfig`.
- `(FileSourceConfig sourceConfig, SchedulerConfig schedulerConfig)` — primary constructor.
- `(SchedulerConfig schedulerConfig, FileSourceConfig sourceConfig)` — alternate arg order.

### Adding a new ProcessFunction

Extend `ProcessFunction<IN, OUT>` and override `processElement`. Chain it onto the stream in `TruffleJob` with `.process(new YourFunction())`. The existing `ProcessFunctionJava` is the reference example.

### Adding a new CSV source

Add the file to `src/main/resources/data/` and append its name to `TruffleJob.CSV_RESOURCES`. Header detection in `ProcessFunctionJava` is based on the literal string `"transactionId"` — update that check if a new source uses a different header.
