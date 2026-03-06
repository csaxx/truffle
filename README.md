# Truffle

An Apache Flink 2.x streaming job that demonstrates hot-reloadable Python and Groovy transform
logic embedded in a running Java job via GraalVM's polyglot API (GraalPy).

The primary goal is to allow Flink `ProcessFunction` logic to be written in Python and executed
inside a running Flink job without recompiling or redeploying the JAR. Scripts are loaded from a
configurable source (classpath, Git, S3, local filesystem, or in-memory map) and reloaded
automatically at a configurable interval.

---

## Prerequisites

- **GraalVM JDK 21** at `E:\code\dist\jvm\graalvm-jdk-21.0.10+8.1`
- Maven is managed automatically by the Maven Wrapper (`mvnw.cmd`); no manual installation needed.
  On first run it downloads Maven 3.9.12 into `~/.m2/wrapper/dists/`.

---

## Building and Running

Use `run_job.bat` for all builds and runs. It sets `JAVA_HOME` and delegates to `mvnw.cmd`.

| Command | Description |
|---|---|
| `run_job.bat compile` | Compile |
| `run_job.bat test` | Run all tests |
| `run_job.bat exec` | Run the job (writes `output/java/` and `output/python/`) |
| `run_job.bat exec-git` | Run the job loading scripts from a Git repo |
| `run_job.bat package -DskipTests` | Build fat JAR for cluster submission |

`exec` uses the `exec:exec` Maven goal, which spawns a child JVM. This is required for Flink 2.x
classloader isolation — without it, GraalVM's polyglot engine cannot initialize correctly.

> **Note:** After deleting resource files, run `run_job.bat clean test` rather than just `test`
> to flush stale compiled classes from `target/`.

---

## IntelliJ IDEA Setup

Four shared run configurations are committed under `.idea/runConfigurations/` and appear
automatically in the Run/Debug selector (`Maven compile`, `Maven test`, `Maven exec`, `Maven exec-git`).
They require a one-time per-machine setup:

1. **Maven home path** — Settings → Build, Execution, Deployment → Build Tools → Maven →
   *Maven home path*: `C:\Users\csa\.m2\wrapper\dists\apache-maven-3.9.12`

2. **Runner JRE** — Settings → Build, Execution, Deployment → Build Tools → Maven → Runner →
   *JRE*: pick **GraalVM JDK 21** (`E:\code\dist\jvm\graalvm-jdk-21.0.10+8.1`).
   If not listed, add it first via File → Project Structure → SDKs.

---

## Architecture

### Pipeline

```
src/main/resources/data/sales_q{1,2,3}.csv
  → TruffleJob#loadCsvLines()         loads all quarterly files into List<String>
  → TruffleJob#runTransform(lines, fn)
      → env.fromData(lines)           bounded DataStream<String>, parallelism 1
      → .process(fn)                  ProcessFunctionJava or ProcessFunctionPython
      → executeAndCollect()           triggers execution, collects results locally
  → TruffleJob#writeOutput()
  → output/java/sales_transformed.csv     (ProcessFunctionJava)
  → output/python/sales_transformed.csv   (ProcessFunctionPython)
```

### Key design decisions

- **No Flink sink.** Results are collected via `DataStream.executeAndCollect()` and written to
  disk with plain Java I/O. This avoids the deprecated `SinkFunction`/`RichSinkFunction` API.
- **Parallelism 1.** Set explicitly to keep local output deterministic and single-file.
- **CSV headers filtered in `processElement`.** All three quarterly files are concatenated;
  `ProcessFunctionJava` drops rows containing `"transactionId"`.
- **Python delegates entirely to Python.** `ProcessFunctionPython.processElement` iterates all
  loaded Python files and calls each file's `process_element(line, out)` in order. The Python
  function calls `out.collect(...)` directly via GraalPy polyglot interop — all output routing
  logic lives in Python.

### Interpreter layer

Two interpreters provide named-context management for embedded scripts:

- **`PolyglotInterpreter`** — wraps GraalVM's polyglot API for any Truffle language (Python,
  JS, Ruby, etc.). Each source file gets its own isolated `Context`, preventing name collisions.
  Contexts sharing the same `(TruffleLanguage, PolyglotContextConfig)` pair share a static
  `Engine` so compiled ASTs are cached across contexts. The engine key includes the config
  because GraalVM requires all contexts on the same engine to use the same host-access policy.

- **`GroovyInterpreter`** — the same conceptual API adapted for Groovy, which runs natively on
  the JVM via `GroovyShell` and cannot use the GraalVM polyglot API. `reset()` recreates the
  shell to prevent `GroovyClassLoader` from accumulating stale class definitions across reload
  cycles — important for long-running processes.

### File loading and hot-reload

```
FileSource          listFiles() + readFile(name) — supplies raw content
    ↓
FileLoader          mtime-based cache; load() never throws; fires change listener
    ↓
ScheduledReloader   periodic polling + callback on change; synchronous initial load
    ↓
ProcessFunctionPython.processElement()
```

`ScheduledReloader.start()` performs the initial load synchronously on the calling thread,
guaranteeing scripts are ready before Flink calls `processElement`. Background polling then
runs on a single daemon thread at `SchedulerConfig.interval()`.

**Grace period.** `SchedulerConfig(interval, gracePeriod)` — if background reloads fail
continuously for longer than `gracePeriod`, a fatal error is stored and re-thrown by
`processElement`, failing the Flink task. Setting `gracePeriod` to `Duration.ZERO` (the default)
means errors are logged but never fatal.

### FileSource implementations

| Source | When to use |
|---|---|
| `ResourceSource` | Bundled scripts in the JAR (classpath `python/` dir) — default |
| `GitSource` | Load scripts directly from a GitHub / GitLab / Gitea repo via HTTP; no clone needed |
| `S3Source` | AWS S3 or MinIO; use `S3SourceConfig.forAws()` or `.forMinio()` |
| `FileSystemSource` | Local directory with optional `WatchService` for push notifications |
| `MapFileSource` | In-memory map; useful for tests or programmatic script injection |

All sources auto-discover files by walking their source — no `index.txt` needed. Filter with
`filemasks` (filename patterns) and `excludeFilemasks` (path-component patterns).

`FileSystemSource` supports push notifications: when a file changes on disk, it calls the
registered change listener directly rather than waiting for the next poll interval.

---

## Test Suite

| Test class | Coverage |
|---|---|
| `ProcessFunctionEquivalenceTest` | **Correctness baseline** — 20-row input, asserts Java/Python output identical row-by-row |
| `PolyglotInterpreterLoadTest` | Context loading, member discovery, duplicate key rejection |
| `PolyglotInterpreterExecuteTest` | execute/executeAll/executeAllPresent variants, error cases |
| `GroovyInterpreterLoadTest` | Mirrors polyglot load tests; validates Groovy method filter (excludes `run`, synthetic methods) |
| `GroovyInterpreterExecuteTest` | Mirrors polyglot execute tests; adds `executeAllPresent` skip-on-absent coverage |
| `FileLoaderTest` | Cases 1–4 (removed/added/unchanged/changed files), status tracking, callback, push-notification via `NotifyingSource` |
| `FileLoaderResultTest` | `LoadResult` fields and `FileLoadCallback` dispatch (12 tests, success/failure × all combinations) |
| `PolyglotContextConfigTest` | Config record fields, `applyTo()` builder integration |

---

## Adding Scripts

**Python:** Create `.py` with a `process_element(line, out)` function in
`src/main/resources/python/`. Auto-discovered; no Java changes required. The function receives
the raw CSV line as a string and the Flink `Collector<String>`; call `out.collect(...)` to emit rows.

**External source:** Construct `ProcessFunctionPython` with a `GitSourceConfig`, `S3SourceConfig`,
or `FileSystemSourceConfig` instead of the default `ResourceSourceConfig`.

---

## Flink Version

Controlled by `<flink.version>` in `pom.xml` (currently `2.2.0`).
