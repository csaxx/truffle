# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Use `run_tests.bat` instead of calling `mvn` directly. It sets `JAVA_HOME` and
the full Maven path automatically (both are needed on this machine).

```bat
rem Compile
run_tests.bat compile

rem Run tests
run_tests.bat test

rem Run the job (writes output/v1/ and output/v2/)
rem exec:exec spawns a child JVM so Flink 2.x classloader isolation works correctly.
run_tests.bat exec

rem Build a fat JAR for cluster submission
run_tests.bat package -DskipTests
```

### Environment notes (Windows)

- **`mvn` is not on PATH** — use `run_tests.bat` or the full path:
  `C:\Users\csa\.m2\wrapper\dists\apache-maven-3.6.3-bin\1iopthnavndlasol9gbrbg6bf2\apache-maven-3.9.12\bin\mvn.cmd`
- **JAVA_HOME must be set** to the GraalVM JDK before invoking Maven, otherwise
  the compiler reports "invalid target release: 21".
  Path: `C:\Program Files\Java\graalvm-jdk-21.0.10+8.1`
- `run_tests.bat` handles both of the above.

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
Two implementations ship with the project:

| Implementation | Source | Constructor |
|----------------|--------|-------------|
| `ResourcePythonSource` | Classpath resources (JAR) | `new ResourcePythonSource("python")` |
| `GitPythonSource` | GitHub / GitLab via HTTP | `new GitPythonSource(url, dir, branch, token)` |
| `S3PythonSource` | AWS S3 / MinIO | `new S3PythonSource(s3Client, bucket, prefix)` |
| `FilePythonSource` | Local filesystem + WatchService | `new FilePythonSource(Path.of("/opt/scripts"))` |

**`ResourcePythonSource`** reads `{directory}/index.txt` from the classpath, then loads
each listed `.py` file from `{directory}/{name}`. Used in production by
`ProcessFunctionPython` and in tests via `SwitchablePythonSource`.

**`GitPythonSource`** fetches `{directory}/index.txt` and each listed file directly from
a Git repository via raw-content HTTP — no clone required. Auto-detects the provider:
- **GitHub** (`github.com`): `https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{directory}/{file}`
- **GitLab** (any other host): `https://{host}/{path}/-/raw/{branch}/{directory}/{file}`

Authentication: pass a personal access token (GitHub PAT or GitLab PAT) as the `token`
argument; pass `null` for public repositories. The token is sent as
`Authorization: Bearer {token}`.

**`S3PythonSource`** reads `{prefix}/index.txt` and each listed file as S3 objects via
the AWS SDK v2 `S3Client`. The client is injected by the caller, keeping credential and
endpoint wiring out of the class:
- **AWS S3**: `S3Client.create()` (uses default credential chain and region).
- **MinIO**: build with `.endpointOverride(...)`, `.forcePathStyle(true)`, and explicit
  `credentialsProvider`. MinIO accepts any region string.
An empty `prefix` fetches objects directly from the bucket root.

**`FilePythonSource`** reads `index.txt` and `.py` files from a local directory using
`Files.readString`. It implements the push-notification protocol: `setChangeListener`
starts a daemon `WatchService` thread that monitors the directory for `ENTRY_CREATE`,
`ENTRY_MODIFY`, and `ENTRY_DELETE` events. When a `.py` file or `index.txt` changes, the
watcher debounces 100 ms then calls the registered callback, which triggers
`GraalPyInterpreter.reload()` automatically — no manual polling needed.
Call `close()` (or use try-with-resources) to stop the watcher thread.

**Push-notification protocol** (`setChangeListener`). `PythonSource` now extends
`Closeable` and has two default methods: `setChangeListener(Runnable onChanged)` (no-op)
and `close()` (no-op). `GraalPyInterpreter` calls `setChangeListener` in its constructor,
passing a callback that invokes `reload()`. Sources that can detect changes (`FilePythonSource`)
override `setChangeListener` to start a watcher; pull-only sources (`ResourcePythonSource`,
`GitPythonSource`, `S3PythonSource`) inherit the no-op default and remain unchanged.
`GraalPyInterpreter.close()` now also calls `source.close()` to stop any watcher thread.

**Generation counter.** `GraalPyInterpreter` exposes `getGeneration()` — a `volatile long`
incremented on every successful reload that detected changes. `ProcessFunctionPython`
caches `lastGeneration` and lazily refreshes its `Value` references at the top of
`processElement` when the generation has advanced, avoiding stale references to closed
Python contexts after a background reload.

**Adding a new `PythonSource`.** Implement `listFiles()` and `readFile(name)` and pass
an instance to `new GraalPyInterpreter(source)`. No other changes needed.

**`getMembers(String memberName)`** streams over all file contexts in index order and
returns a `List<Value>` — one entry per file — for the named Python binding. The caller
(e.g. `ProcessFunctionPython`) iterates the list to invoke every file's function.

**Hot reload.** `reload()` re-reads `index.txt`, computes SHA-256 hashes of each file's
content, and reconciles:
- Removed files → their `Context` is closed and dropped from the map.
- New or changed files → old `Context` closed (if any), new `Context` created and
  evaluated. The engine cache means unchanged peer files pay no recompile cost.
- Unchanged files → their `Context` is reused as-is.
Returns `true` if anything changed so the caller can re-fetch its `Value` references.

**Adding a new Python transform file.** Create the `.py` file under
`src/main/resources/python/` with a `process_element(line, out)` function, then add
its filename to `python/index.txt`. No Java changes are required. The function receives
the raw CSV line as a string and the Flink `Collector<String>`; call `out.collect(...)`
to emit output rows.

### Adding a new ProcessFunction

Extend `ProcessFunction<IN, OUT>` and override `processElement`. Chain it onto the stream in `TruffleJob` with `.process(new YourFunction())`. The existing `SalesTransformFunction` is the reference example.

### Adding a new CSV source

Add the file to `src/main/resources/` and append its name to `TruffleJob.CSV_RESOURCES`. Header detection in `SalesTransformFunction` is based on the literal string `"transactionId"` — update that check if a new source uses a different header.
