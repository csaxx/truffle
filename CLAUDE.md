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

- **`mvn` is not on PATH** — use `run_job.bat` or the full path:
  `C:\Users\csa\.m2\wrapper\dists\apache-maven-3.6.3-bin\1iopthnavndlasol9gbrbg6bf2\apache-maven-3.9.12\bin\mvn.cmd`
- **JAVA_HOME must be set** to the GraalVM JDK before invoking Maven, otherwise
  the compiler reports "invalid target release: 21".
  Path: `C:\Program Files\Java\graalvm-jdk-21.0.10+8.1`
- `run_job.bat` handles both of the above.

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
manages the lifecycle of GraalPy execution contexts for all Python files.

**Per-file context isolation.** Each `.py` file listed in `python/index.txt` gets its
own `Context`. This prevents name collisions — two files can both define
`process_element` without one overwriting the other. All contexts share a single static
`Engine` (`SHARED_ENGINE`) so compiled ASTs are cached across contexts; the per-file
overhead is only interpreter state, which is negligible at this scale.

**Index-driven loading.** `python/index.txt` is the authoritative list of files to
load, in execution order. Lines starting with `#` and blank lines are ignored.
Files are resolved as classpath resources under `python/`.

**PythonSource abstraction.** `GraalPyInterpreter` does not load files directly.
It accepts a `PythonSource` (in `org.csa.truffle.graal.source`) at construction time.
The interface has two methods: `listFiles()` returns the ordered filename list;
`readFile(name)` returns file content. `reload()` is no-arg and queries the injected source.
`getDataAge()` returns `Optional<Instant>` — the latest modification timestamp
across all currently listed files, or `Optional.empty()` if the source cannot
determine this. Called by `GraalPyInterpreter` at the end of every reload to
populate `ReloadResult.dataAge()`. `FilePythonSource` implements it via
`Files.getLastModifiedTime()`; `S3PythonSource` via `HeadObjectRequest`
(metadata-only, no download). `ResourcePythonSource` and `GitPythonSource`
inherit the no-op default.
Four implementations ship with the project, split into subpackages:

| Implementation (package) | Source | Config record |
|--------------------------|--------|---------------|
| `resource.ResourcePythonSource` | Classpath JAR | `new ResourceSourceConfig("python")` |
| `resource.GitPythonSource` | GitHub / GitLab / Gitea via HTTP | `new GitSourceConfig(url, dir, branch, token, forge)` |
| `s3.S3PythonSource` | AWS S3 / MinIO | `new S3SourceConfig(...)` or static helpers |
| `file.FilePythonSource` | Local filesystem + WatchService | `new FileSourceConfig(path, watch)` |

**Config records and `PythonSourceFactory`.** `PythonSourceConfig` is a marker interface
(in `org.csa.truffle.graal.source`) extended by the four config record types above. All
record components are primitives, Strings, or enums — serialization is guaranteed.
`PythonSourceFactory.create(PythonSourceConfig)` is a `final class` with a static factory
method; it uses a `switch` on the concrete type to instantiate the correct `PythonSource`. S3 credential
wiring and `Path` conversion live here, keeping the source classes free of construction
details. `ProcessFunctionPython` stores a `PythonSourceConfig` (not a lambda); `open()`
calls the factory to build the source. The default constructor uses
`new ResourceSourceConfig("python")`.

**`resource.ResourcePythonSource`** reads `{directory}/index.txt` from the classpath, then
loads each listed `.py` file from `{directory}/{name}`. Used in production by
`ProcessFunctionPython` and in tests via `SwitchablePythonSource`.

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
registered callback, which triggers `GraalPyInterpreter.reload()` automatically — no
manual polling needed. Call `close()` (or use try-with-resources) to stop the watcher thread.

**Push-notification protocol** (`setChangeListener`). `PythonSource` extends `Closeable`
and has two default methods: `setChangeListener(Runnable onChanged)` (no-op) and `close()`
(no-op). `GraalPyInterpreter` calls `setChangeListener` in its constructor, passing a
callback that invokes `reload()`. Sources that can detect changes (`FilePythonSource`)
override `setChangeListener` to start a watcher; pull-only sources (`ResourcePythonSource`,
`GitPythonSource`, `S3PythonSource`) inherit the no-op default and remain unchanged.
`GraalPyInterpreter.close()` also calls `source.close()` to stop any watcher thread.

**Generation counter.** `GraalPyInterpreter` exposes `getGeneration()` — a `volatile long`
incremented on every successful reload that detected changes. `ProcessFunctionPython`
caches `lastGeneration` and lazily refreshes its `Value` references at the top of
`processElement` when the generation has advanced, avoiding stale references to closed
Python contexts after a background reload.

**Adding a new `PythonSource`.** Implement `listFiles()` and `readFile(name)` in a new
class (place it in an appropriate `graal/source/` subpackage). Also create a config
record implementing `PythonSourceConfig`, add a corresponding `case` to
`PythonSourceFactory.create()`, and pass the config to
`new ProcessFunctionPython(yourConfig)`. No other changes needed.
If your source can efficiently retrieve file modification times (e.g. a database column
or object-store `HeadObject`), also override `getDataAge()` to return
`Optional.of(latestInstant)`. Otherwise inherit the no-op default, which causes
`ReloadResult.dataAge()` to be empty for that source.

**`getMembers(String memberName)`** streams over all file contexts in index order and
returns a `List<Value>` — one entry per file — for the named Python binding. The caller
(e.g. `ProcessFunctionPython`) iterates the list to invoke every file's function.

**Hot reload.** `reload()` re-reads `index.txt`, computes SHA-256 hashes of each file's
content, and reconciles:
- Removed files → their `Context` is closed and dropped from the map.
- New or changed files → old `Context` closed (if any), new `Context` created and
  evaluated. The engine cache means unchanged peer files pay no recompile cost.
- Unchanged files → their `Context` is reused as-is.
Returns a `ReloadResult` record with three fields:
- `changed` — `true` if any file was added, removed, or changed.
- `reloadedAt` — the `Instant` captured at the top of the reload, before any I/O.
- `dataAge` — `Optional<Instant>` from `PythonSource.getDataAge()` — the latest
  file mtime across all loaded files; empty for JAR/Git sources.

Callers use `result.changed()` to decide whether to re-fetch `Value` references.

**Adding a new Python transform file.** Create the `.py` file under
`src/main/resources/python/` with a `process_element(line, out)` function, then add
its filename to `python/index.txt`. No Java changes are required. The function receives
the raw CSV line as a string and the Flink `Collector<String>`; call `out.collect(...)`
to emit output rows.

### Scheduled reload

**`SchedulerConfig`** (`src/main/java/org/csa/truffle/graal/reload/SchedulerConfig.java`)
is a `Serializable` record with a single `Duration interval` component. Passing a config
object (rather than a raw `Duration`) to `ProcessFunctionPython` mirrors the `PythonSourceConfig`
pattern and ensures the value survives Flink serialization across distributed operators.

**`ReloadResult`** (`src/main/java/org/csa/truffle/graal/reload/ReloadResult.java`)
is the return type of `GraalPyInterpreter.reload()`. It bundles:
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
- `close()` calls `executor.shutdownNow()`. Always close via try-with-resources or via
  `ProcessFunctionPython.close()`, which calls both `reloader.close()` and `interpreter.close()`.

**`ProcessFunctionPython` constructors:**
- `()` — defaults to `new SchedulerConfig(Duration.ofMinutes(5))` + classpath `python/` dir.
- `(Duration interval)` — convenience overload; wraps in `SchedulerConfig`.
- `(SchedulerConfig schedulerConfig, PythonSourceConfig sourceConfig)` — primary constructor.

### Adding a new ProcessFunction

Extend `ProcessFunction<IN, OUT>` and override `processElement`. Chain it onto the stream in `TruffleJob` with `.process(new YourFunction())`. The existing `SalesTransformFunction` is the reference example.

### Adding a new CSV source

Add the file to `src/main/resources/` and append its name to `TruffleJob.CSV_RESOURCES`. Header detection in `SalesTransformFunction` is based on the literal string `"transactionId"` — update that check if a new source uses a different header.
