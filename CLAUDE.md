# CLAUDE.md

## Build Commands

Use `run_job.bat` (never call `mvn` directly — not on PATH; sets JAVA_HOME automatically).

```bat
run_job.bat compile
run_job.bat test
run_job.bat exec           # writes output/java/ and output/python/
run_job.bat exec-git       # loads scripts from Git repo
run_job.bat package -DskipTests
```

After deleting resources, run `run_job.bat clean test` (not just `test`).

**JAVA_HOME:** `E:\code\dist\jvm\graalvm-jdk-21.0.10+8.1` (GraalVM JDK 21)

## Formatting

IntelliJ IDEA default Java formatting style.

## Architecture

Flink 2.x streaming job. Python/Groovy transform scripts are hot-reloadable via GraalVM polyglot (GraalPy).

**Pipeline:** `data/sales_q{1,2,3}.csv → TruffleJob → ProcessFunctionJava / ProcessFunctionPython → output/{java,python}/sales_transformed.csv`

Results collected via `executeAndCollect()` (no Flink sink). Parallelism 1.

**Correctness baseline:** `ProcessFunctionEquivalenceTest` — asserts Java/Python output parity on a 20-row input.

## PolyglotInterpreter

`src/main/java/org/csa/truffle/interpreter/PolyglotInterpreter.java` — GraalVM polyglot context manager. Each source gets its own isolated `Context`; contexts sharing `(TruffleLanguage, PolyglotContextConfig)` share a static `Engine` for AST caching.

```java
new PolyglotInterpreter()                        // uses MINIMAL config
new PolyglotInterpreter(PolyglotContextConfig)
addContext(language, name, content)              // name = unique key, determines index order
```

**PolyglotContextConfig** (`src/main/java/org/csa/truffle/interpreter/polyglot/PolyglotContextConfig.java`) — Serializable record with `hostAccess`, `allowHostClassLookup`, `ioAccess`, `allowNativeAccess`, `allowCreateThread`, `polyglotAccess`.
- `MINIMAL` — `HostAccess.ALL` (scripts may call `out.collect()`), no class lookup, `IOAccess.NONE` (default)
- `FULL` — all permissions enabled
- `SANDBOXED` — `HostAccess.NONE`, pure sandbox

**API:** `hasContext`, `getContexts`, `getContext`, `hasMember`, `getMemberNames`, `getMember`, `getMembers`, `canExecute`, `execute`, `executeVoid`, `executeAll`, `executeAllVoid`, `executeAllPresent`, `executeAllVoidPresent`, `reset`, `close`, `closeSharedEngines` (static — JVM shutdown only).

## GroovyInterpreter

`src/main/java/org/csa/truffle/interpreter/GroovyInterpreter.java` — Groovy equivalent of `PolyglotInterpreter`, runs via `GroovyShell` (JVM-native, not Truffle).

```java
new GroovyInterpreter()
addContext(name, content)    // no language param
```

Returns `GroovyCallable` (not `Value`) from `getMember`; returns `Object` (not `Value`) from `execute`. `reset()` recreates the shell to prevent class loader leaks across reload cycles. `canExecute` always `true` if member exists; throws `NoSuchElementException` if absent.

## FileSource

`src/main/java/org/csa/truffle/source/` — supplies file contents to `FileLoader`. All implementations auto-discover files (no `index.txt`).

| Class | Config | Source |
|---|---|---|
| `ResourceSource` | `ResourceSourceConfig(dir, filemasks[], excludeFilemasks[])` | Classpath JAR |
| `GitSource` | `GitSourceConfig(url, dir, branch, token, forge)` | GitHub / GitLab / Gitea via HTTP |
| `S3Source` | `S3SourceConfig.forAws(bucket, prefix)` / `.forMinio(...)` | AWS S3 / MinIO |
| `FileSystemSource` | `FileSystemSourceConfig(path, watch)` | Local filesystem + WatchService |
| `MapFileSource` | `MapFileSourceConfig(filemasks[])` | In-memory; `put/remove/triggerChange` |

`filemasks` matches any pattern against the filename; `excludeFilemasks` excludes if any pattern matches any path component. `FileSourceFactory.create(config)` instantiates the correct source.

**Adding a new FileSource:** implement `listFiles()` + `readFile(name)`, create a config record implementing `FileSourceConfig`, add a `case` to `FileSourceFactory.create()`.

## FileLoader

`src/main/java/org/csa/truffle/loader/FileLoader.java` — mtime-based content cache over a `FileSource`.

```java
new FileLoader(source)
new FileLoader(source, callback)    // FileLoadCallback fired on every load()
```

`load()` never throws — errors go into `LoadResult.error`. `getFileContents()` returns a `LinkedHashMap` snapshot in index order. `getStatus()` returns `FileLoaderStatus` (`lastCheckedAt`, `lastChangedAt`, `lastSuccessAt`, `loadedFiles`, error fields). Registers `this::load` as the source's push-change listener.

## ScheduledReloader

`src/main/java/org/csa/truffle/scheduler/ScheduledReloader.java` — drives periodic `FileLoader` polls; fires `ScheduledReloadCallback` only when changes are detected.

```java
new ScheduledReloader(FileSourceConfig|FileSource|FileLoader, SchedulerConfig [, PolyglotContextConfig], callback)
```

`SchedulerConfig(interval)` — `gracePeriod` defaults to `Duration.ZERO` (errors never fatal). When positive, reload failures exceeding the grace period store a `fatalError`; `ProcessFunctionPython.processElement()` re-throws it.

`start()` — synchronous initial load on calling thread, then schedules background polling. Methods: `checkForFatalError()`, `getStatus()`, `getFatalError()`, `getFirstErrorAt()`, `close()`.

**Callback:** `void onReload(FileLoaderStatus, GraalPyInterpreter)` — fired only on changes; `interpreter = null` when grace period exceeded (caller should clean up).

## ProcessFunctionPython constructors

```java
new ProcessFunctionPython()                           // 5-min interval, classpath python/
new ProcessFunctionPython(Duration interval)
new ProcessFunctionPython(FileSourceConfig, SchedulerConfig)
new ProcessFunctionPython(SchedulerConfig, FileSourceConfig)
```

Default config excludes `flink_types.py` and `venv/` subtrees.

## Extension points

**New Python transform:** create `.py` with `process_element(line, out)` in `src/main/resources/python/`. Auto-discovered; no Java changes needed.

**New ProcessFunction:** extend `ProcessFunction<IN, OUT>`, override `processElement`, chain in `TruffleJob` with `.process(new YourFunction())`.

**New CSV source:** add file to `src/main/resources/data/`, append name to `TruffleJob.CSV_RESOURCES`. Header detection uses `"transactionId"` — update if different.
