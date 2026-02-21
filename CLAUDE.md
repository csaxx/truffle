# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Compile
mvn compile

# Run tests
mvn test

# Run the job (writes output/v1/ and output/v2/)
# exec:exec spawns a child JVM so Flink 2.x classloader isolation works correctly.
mvn exec:exec

# Build a fat JAR for cluster submission
mvn package -DskipTests
```

`mvn` must be on PATH. Java 11+ is required.

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

### Adding a new ProcessFunction

Extend `ProcessFunction<IN, OUT>` and override `processElement`. Chain it onto the stream in `TruffleJob` with `.process(new YourFunction())`. The existing `SalesTransformFunction` is the reference example.

### Adding a new CSV source

Add the file to `src/main/resources/` and append its name to `TruffleJob.CSV_RESOURCES`. Header detection in `SalesTransformFunction` is based on the literal string `"transactionId"` — update that check if a new source uses a different header.
