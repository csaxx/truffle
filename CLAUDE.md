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
classpath CSVs (sales_q{1,2,3}.csv)
  → TruffleJob#loadCsvLines()        // loads lines into List<String>
  → TruffleJob#runTransform(lines, fn)
      → env.fromData(lines)          // bounded DataStream<String>
      → .process(fn)                 // SalesTransformFunction or V2
      → executeAndCollect()          // triggers execution, collects locally
  → TruffleJob#writeOutput()         // writes CSV header + rows
  → output/v1/sales_transformed.csv  (SalesTransformFunction)
  → output/v2/sales_transformed.csv  (SalesTransformFunctionV2)
```

### Key design choices

- **No Flink sink:** results are collected via `DataStream.executeAndCollect()` and written to disk with plain Java I/O. This avoids the deprecated `SinkFunction` / `RichSinkFunction` API from Flink 1.x.
- **Parallelism 1:** set explicitly in `TruffleJob` to keep local output deterministic and single-file.
- **CSV resources are loaded before the Flink graph is built** — the driver reads them from the classpath into a `List<String>` and feeds the list to `env.fromData()`. All three quarterly files are concatenated; header rows are filtered inside `SalesTransformFunction.processElement`.
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
binding, or `null` if the module does not define that name. Results are cached on first
access via `computeIfAbsent`; a missing member causes a polyglot lookup on each call
(absent members are the exception, not the norm).

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
  in the given file, or `null` if the file is not loaded or does not define that member.
- `getMembers(memberName)` — returns one `Value` per file that defines `memberName`,
  in index order; files that do not define the member are omitted.
- `execute(filename, memberName, args...)` — calls the named function in the given file;
  no-op if the file or member is absent.
- `executeAll(memberName, args...)` — calls the named function in every file that defines
  it, in index order.
- `close()` — closes all per-file `Context`s. Use try-with-resources.

**Adding a new Python transform file.** Create the `.py` file under
`src/main/resources/python/` with a `process_element(line, out)` function, then add
its filename to `python/index.txt`. No Java changes are required. The function receives
the raw CSV line as a string and the Flink `Collector<String>`; call `out.collect(...)`
to emit output rows.

### PythonSource implementations

These classes (in `org.csa.truffle.graal.source`) are used by `ProcessFunctionPython`
via `PythonSourceFactory` to supply Python file contents. They are separate from the
`FileSource` hierarchy used by `FileLoader`.

**`PythonSource` interface.** `listFiles()` returns the ordered filename list;
`readFile(name)` returns file content. `getDataAge()` returns `Optional<Instant>` —
the latest modification timestamp across all currently listed files, or
`Optional.empty()` if the source cannot determine this. `FilePythonSource` implements
it via `Files.getLastModifiedTime()`; `S3PythonSource` via `HeadObjectRequest`
(metadata-only, no download). `ResourcePythonSource` and `GitPythonSource` inherit
the no-op default.

Four implementations ship with the project, split into subpackages:

| Implementation (package) | Source | Config record |
|--------------------------|--------|---------------|
| `resource.ResourcePythonSource` | Classpath JAR | `new ResourceSourceConfig("python")` |
| `resource.GitPythonSource` | GitHub / GitLab / Gitea via HTTP | `new GitSourceConfig(url, dir, branch, token, forge)` |
| `s3.S3PythonSource` | AWS S3 / MinIO | `new S3SourceConfig(...)` or static helpers |
| `file.FilePythonSource` | Local filesystem + WatchService | `new FileSourceConfig(path, watch)` |

**Config records and `PythonSourceFactory`.** `PythonSourceConfig` is a marker interface
extended by the four config record types above. All record components are primitives,
Strings, or enums — serialization is guaranteed. `PythonSourceFactory.create(PythonSourceConfig)`
is a `final class` with a static factory method; it uses a `switch` on the concrete type
to instantiate the correct `PythonSource`. S3 credential wiring and `Path` conversion live
here, keeping the source classes free of construction details. `ProcessFunctionPython` stores
a `PythonSourceConfig` (not a lambda); `open()` calls the factory to build the source. The
default constructor uses `new ResourceSourceConfig("python")`.

**`resource.ResourcePythonSource`** reads `{directory}/index.txt` from the classpath, then
loads each listed `.py` file from `{directory}/{name}`.

**`resource.GitPythonSource`** fetches `{directory}/index.txt` and each listed file
directly from a Git repository via raw-content HTTP — no clone required. The forge is
identified by `GitForgeType` (GITHUB, GITLAB, GITEA/Forgejo); pass `null` in
`GitSourceConfig` to auto-detect from the URL:
- **GitHub** (`github.com`): `https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{directory}/{file}`
- **GitLab** (other hosts): `https://{host}/{path}/-/raw/{branch}/{directory}/{file}`
- **Gitea / Forgejo**: `https://{host}/{path}/raw/branch/{branch}/{directory}/{file}`

Authentication: set `token` in `GitSourceConfig`; pass `null` for public repositories.
The token is sent as `Authorization: Bearer {token}`.

**`s3.S3PythonSource`** reads `{prefix}/index.txt` and each listed file as S3 objects via
the AWS SDK v2 `S3Client`. `S3SourceConfig` has two static helpers:
- `S3SourceConfig.forAws(bucket, prefix)` — AWS S3 with the default credential chain.
- `S3SourceConfig.forMinio(endpoint, bucket, prefix, accessKeyId, secretKey)` — MinIO /
  custom endpoint with explicit credentials.
An empty `prefix` fetches objects directly from the bucket root.
`PythonSourceFactory` builds the `S3Client` from config fields; callers need not touch
the AWS SDK directly.

**`file.FilePythonSource`** reads `index.txt` and `.py` files from a local directory using
`Files.readString`. The `watch` boolean in `FileSourceConfig` controls whether it starts
a `WatchService` thread. When watching, `setChangeListener` starts a daemon thread that
monitors the directory for `ENTRY_CREATE`, `ENTRY_MODIFY`, and `ENTRY_DELETE` events.
When a `.py` file or `index.txt` changes, the watcher debounces 100 ms then calls the
registered callback. Call `close()` (or use try-with-resources) to stop the watcher thread.

**Push-notification protocol** (`setChangeListener`). `PythonSource` extends `Closeable`
and has two default methods: `setChangeListener(Runnable onChanged)` (no-op) and `close()`
(no-op). Sources that can detect changes (`FilePythonSource`) override `setChangeListener`
to start a watcher; pull-only sources inherit the no-op default.

**Adding a new `PythonSource`.** Implement `listFiles()` and `readFile(name)` in a new
class (place it in an appropriate `graal/source/` subpackage). Also create a config
record implementing `PythonSourceConfig`, add a corresponding `case` to
`PythonSourceFactory.create()`, and pass the config to
`new ProcessFunctionPython(yourConfig)`. No other changes needed.
If your source can efficiently retrieve file modification times, also override
`getDataAge()` to return `Optional.of(latestInstant)`. Otherwise inherit the no-op
default, which causes `ReloadResult.dataAge()` to be empty for that source.

### FileLoader

**`FileLoader`** (`src/main/java/org/csa/truffle/loader/FileLoader.java`) is a `Closeable`
content cache that reads `.py` files from a `FileSource` and tracks changes via per-file
modification timestamps.

**`load()`** (re)reads the file list from `source.listFiles()`. On the first call every file is
read; on subsequent calls a file is re-read only when its mtime has advanced (or the source cannot
provide a timestamp). Files removed from `index.txt` are evicted from the cache. Returns `true`
if any file was added, removed, or had updated content. Fires the `onChanged` callback (if set)
and updates `LoadStatus` on every call regardless of whether a change occurred.

**`getFileContents()`** returns a defensive `LinkedHashMap<String, String>` snapshot in index order.

**`getStatus()`** returns the `LoadStatus` instance. `LoadStatus` fields are all `volatile`:
- `lastCheckedAt` — set on every `load()` call; `null` until the first call.
- `lastChangedAt` — set only when `load()` detects a change.
- `loadedFiles` — `Set<String>` snapshot of currently cached filenames.
- `lastErrorAt` / `lastError` / `firstErrorAt` — error streak tracking (same semantics as `ScheduledReloader`).

**Push-notification.** `FileLoader` calls `source.setChangeListener(this::doReloadOnChange)` in
its constructor. When a push-capable source fires the listener, `doReloadOnChange` calls `load()`
automatically; any `IOException` is caught and logged. Pull-only sources inherit the no-op default
and are unaffected.

**`close()`** delegates to `source.close()`. Always use try-with-resources.

**Constructors:** `FileLoader(FileSource source)` and `FileLoader(FileSource source, Runnable onChanged)`.

**`FileLoaderTest`** (`src/test/java/org/csa/truffle/loader/FileLoaderTest.java`) covers
Cases 1–4 (removed / added / unchanged / changed files; `load()` return value), `LoadStatus`
field tracking, `onChanged` callback firing, and push-notification integration via
`NotifyingSource` — an inner test helper that captures the registered listener and exposes
`triggerChange()` to fire it synchronously without a real watcher. Uses `SwitchableFileSource`
and the existing `python_hr_v1` / `python_hr_v2` test resources.

### Scheduled reload

**`SchedulerConfig`** (`src/main/java/org/csa/truffle/graal/reload/SchedulerConfig.java`)
is a `Serializable` record with two components: `Duration interval` and `Duration gracePeriod`.
Passing a config object (rather than a raw `Duration`) to `ProcessFunctionPython` mirrors the
`PythonSourceConfig` pattern and ensures the value survives Flink serialization across distributed
operators. The single-arg constructor `SchedulerConfig(interval)` defaults `gracePeriod` to
`Duration.ZERO`, which means reload errors are tolerated indefinitely (logged but never fatal).
When `gracePeriod` is positive, background reload failures that persist longer than the grace
period cause `ScheduledReloader` to store a fatal `RuntimeException`; `ProcessFunctionPython`
checks for it at the top of every `processElement()` call and re-throws it, failing the Flink task.

**`ReloadResult`** (`src/main/java/org/csa/truffle/graal/reload/ReloadResult.java`)
is the return type of the reload cycle driven by `ScheduledReloader`. It bundles:
- `changed` (`boolean`) — whether any file changed during this reload.
- `reloadedAt` (`Instant`) — wall-clock time captured at the start of the reload call.
- `dataAge` (`Optional<Instant>`) — latest file mtime from `PythonSource.getDataAge()`;
  empty for JAR-classpath and Git HTTP sources.

**`ScheduledReloader`** (`src/main/java/org/csa/truffle/graal/reload/ScheduledReloader.java`)
wraps a `GraalPyInterpreter` and drives periodic polling:
- `start()` performs an initial **synchronous** reload on the calling thread (guaranteeing
  data is ready before Flink calls `processElement`), then schedules background reloads via
  a single-daemon-thread `ScheduledExecutorService` at `config.interval()`.
- All observable state is `volatile` (immutable `Instant` / record writes are safe):
  - `lastCheckedAt` — set on every reload (changed or not).
  - `lastChangedAt` — set only when `ReloadResult.changed()` is `true`.
  - `lastResult` — the full `ReloadResult` from the most recent reload cycle; `null` until
    first `start()`.
  - `lastErrorAt` / `lastError` — set (and never cleared) when a reload throws `IOException`;
    `null` if no error has occurred.
  - `firstErrorAt` — start of the current error streak; cleared (`null`) when a reload succeeds.
  - `fatalError` — set once when the grace period is exceeded; never cleared.
- When `config.gracePeriod()` is positive and background reloads keep failing, `doReloadQuietly()`
  tracks the streak start in `firstErrorAt`. Once `Duration.between(firstErrorAt, now) >= gracePeriod`,
  it stores a descriptive `RuntimeException` in `fatalError`.
- `checkForFatalError()` — throws the stored `fatalError` if set; no-op otherwise. Called by
  `ProcessFunctionPython.processElement()` on every record to propagate the failure into Flink.
- `getFirstErrorAt()` — returns the start of the current error streak, or `null` if no streak.
- `close()` calls `executor.shutdownNow()`. Always close via try-with-resources or via
  `ProcessFunctionPython.close()`, which calls both `reloader.close()` and `interpreter.close()`.

**`ProcessFunctionPython` constructors:**
- `()` — defaults to `new SchedulerConfig(Duration.ofMinutes(5))` + classpath `python/` dir.
- `(Duration interval)` — convenience overload; wraps in `SchedulerConfig`.
- `(SchedulerConfig schedulerConfig, PythonSourceConfig sourceConfig)` — primary constructor.

> **Note:** `ScheduledReloader` and `ProcessFunctionPython` are pending a follow-up refactor
> to wire through `FileLoader` + `GraalPyInterpreter(Map)` and will not compile until that
> work is complete.

### Adding a new ProcessFunction

Extend `ProcessFunction<IN, OUT>` and override `processElement`. Chain it onto the stream in `TruffleJob` with `.process(new YourFunction())`. The existing `SalesTransformFunction` is the reference example.

### Adding a new CSV source

Add the file to `src/main/resources/` and append its name to `TruffleJob.CSV_RESOURCES`. Header detection in `SalesTransformFunction` is based on the literal string `"transactionId"` — update that check if a new source uses a different header.
