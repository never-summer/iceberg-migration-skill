# iceberg-migration-skill — Examples

Concrete before/after rewrite tables and the multi-table mapping format.
Referenced from [SKILL.md](./SKILL.md). Read this when you need to look up
the exact Iceberg equivalent of a specific Parquet/ORC call site.

## Conversion Reference — Python

| Before (Parquet) | After (Iceberg) |
|---|---|
| `pd.read_parquet(path)` / `pd.read_orc(path)` | `catalog.load_table((ns, name)).scan().to_pandas()` |
| `df.to_parquet(path)` / `df.to_orc(path)` | `tbl.overwrite(df)` |
| `spark.read.parquet(path)` / `.orc(path)` | `spark.table("ns.name")` |
| `spark.read.format("parquet"\|"orc").load(path)` | `spark.table("ns.name")` |
| `df.write.parquet(path)` / `.orc(path)` | `df.writeTo("ns.name").overwritePartitions()` |
| `df.write.format("parquet"\|"orc").save(path)` | `df.writeTo("ns.name").overwritePartitions()` |
| `pq.read_table(path)` / `orc.read_table(path)` | `tbl.scan().to_arrow()` |
| `pq.write_table(table, path)` / `orc.write_table(...)` | `tbl.overwrite(table)` |
| `pq.ParquetFile` / `pq.ParquetDataset` / `pa.dataset.*` | *(TODO comment — rewrite manually)* |
| `spark.readStream.parquet/orc/format(...)` | *(TODO comment — manual migration)* |

## Conversion Reference — Java/Scala Spark

| Before (Java) | After (Iceberg) |
|---|---|
| `spark.read().parquet("p")` / `.orc("p")` | `spark.read().format("iceberg").load("ns.table")` |
| `spark.read().format("parquet"\|"orc").load("p")` | `spark.read().format("iceberg").load("ns.table")` |
| `df.write().mode("overwrite").parquet("p")` / `.orc("p")` | `df.writeTo("ns.table").overwritePartitions()` |
| `df.write()...format("parquet"\|"orc").save("p")` | `df.writeTo("ns.table").overwritePartitions()` |
| `df.write().saveAsTable("t")` *(no partitionBy / bucketBy)* | `df.writeTo("ns.t").createOrReplace()` |
| `df.write().partitionBy(...).saveAsTable("t")` / `.bucketBy(...).saveAsTable("t")` | **Two steps:** (1) pre-create `CREATE TABLE ns.t (...) USING iceberg PARTITIONED BY (...)` with the desired Iceberg partition spec; (2) rewrite call site as `df.writeTo("ns.t").overwritePartitions()`. `createOrReplace()` would silently drop the spec. |
| `df.write().partitionBy("day").parquet(...)` | `df.writeTo("ns.t").overwritePartitions()` *(+ TODO comment — user must pre-create the table with the right partition spec)* |
| `spark.readStream()....parquet\|orc\|format(...)` | *(TODO comment — manual migration)* |

| Before (Scala) | After (Iceberg) |
|---|---|
| `spark.read.parquet("p")` / `.orc("p")` | `spark.read.format("iceberg").load("ns.table")` |
| `spark.read.format("parquet"\|"orc").load("p")` | `spark.read.format("iceberg").load("ns.table")` |
| `df.write.mode("overwrite").parquet("p")` / `.orc("p")` | `df.writeTo("ns.table").overwritePartitions()` |
| `df.write.format("parquet"\|"orc").save("p")` | `df.writeTo("ns.table").overwritePartitions()` |

## Conversion Reference — Hive / SparkSQL

| Before | After |
|---|---|
| `"CREATE TABLE t (...) STORED AS PARQUET\|ORC"` | `"CREATE TABLE t (...) USING iceberg"` |
| `"CREATE [EXTERNAL] TABLE t (...) STORED AS PARQUET LOCATION '...'"` | `"CREATE TABLE t (...) USING iceberg LOCATION '...'"` *(review LOCATION semantics manually)* |
| `"CREATE TABLE t (...) USING parquet\|orc"` | `"CREATE TABLE t (...) USING iceberg"` |
| `df.write().saveAsTable("t")` *(no partitionBy / bucketBy)* | `df.writeTo("ns.t").createOrReplace()` |
| `df.write().bucketBy(...).saveAsTable("t")` / `.partitionBy(...).saveAsTable("t")` | Pre-create `CREATE TABLE ns.t (...) USING iceberg PARTITIONED BY (bucket(N, col))` then `df.writeTo("ns.t").overwritePartitions()`. **Do not** use `createOrReplace()` here — it resets the partition spec. |
| `"INSERT INTO TABLE t ..."` / `"INSERT OVERWRITE TABLE t ..."` | *(no change — Spark handles Iceberg tables via the same SQL, assuming catalog is configured)* |
| Existing Hive table with data | `CALL catalog.system.migrate('db.t')` — manual step |

## Dependencies Added

| Ecosystem | File | Dependency |
|---|---|---|
| Python | `requirements.txt` / `pyproject.toml` | `pyiceberg[sql-sqlite]>=0.7.0` |
| Maven | `pom.xml` | `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0` |
| Gradle | `build.gradle` | `implementation 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0'` |

## Multi-table projects

Projects that touch several logical tables use a **mapping file** to route each path to its Iceberg target. JSON format:

```json
{
  "default": {"namespace": "default", "table": "unmapped"},
  "tables": [
    {"path_glob": "s3://bucket/events/*", "namespace": "analytics", "table": "events"},
    {"path_glob": "*/users/*",            "namespace": "analytics", "table": "users"},
    {"path_glob": "s3://legacy/*",        "skip": true},
    {"path_glob": "s3://bucket/logs/*",   "direction": "write", "namespace": "analytics", "table": "logs"},
    {"path_glob": "s3://bucket/logs/*",   "direction": "read",  "skip": true}
  ]
}
```

- `tables` — ordered list; first matching `path_glob` (fnmatch style) wins.
- `default` — optional fallback target when no glob matches.
- `skip: true` — matching operations are **left as parquet/ORC**. The transformer drops an `iceberg: skipped by mapping` marker above the line so it's obvious on review.
- `direction: "read" | "write" | "any"` (default `"any"`) — restrict an entry to one side only. Paired entries let you migrate writes but keep reads (or vice versa).
- You can also pass `--table`/`--namespace` **alongside** `--mapping` as a CLI-level fallback.

In the markdown-only skill there is no CLI to consume this mapping JSON automatically. The mapping format above is still useful as the **shape** of the per-call-site resolution you record in the migration plan: for each detected call site, decide `(namespace, table)` (or `skip`), note it next to the file:line, and apply the corresponding rewrite by hand. Calls whose path is a variable or doesn't match any rule should be surfaced to the user as unresolved.
