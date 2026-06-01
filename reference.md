# open-table-migrator — Reference

Deep-dive documentation for each subsystem. Referenced from [SKILL.md](./SKILL.md).
Read the section relevant to whichever stage you are in (detection, mapping, dry-run,
runbook, post-migration ops).

## Post-Migration Operational Concerns

The skill rewrites **code**, not the operational model of the tables. After the migration lands, surface these three questions to the user — they change how the tables behave in production.

### 1. Format version & write mode (Copy-on-Write vs Merge-on-Read)

MoR/CoW is a **table property**, not a code choice. `INSERT` / `append` / `overwritePartitions()` are unaffected — the mode only changes how `UPDATE` / `DELETE` / `MERGE INTO` materialize changes.

```sql
ALTER TABLE ns.events SET TBLPROPERTIES (
  'format-version'       = '2',               -- required for MoR
  'write.update.mode'    = 'merge-on-read',   -- or 'copy-on-write'
  'write.delete.mode'    = 'merge-on-read',
  'write.merge.mode'     = 'merge-on-read'
);
```

| | Copy-on-Write (default pre-v2) | Merge-on-Read (v2) |
|---|---|---|
| What is written on UPDATE/DELETE | Fully rewritten data files | Original files + position/equality delete files |
| Write latency | High (rewrite partition) | Low |
| Read latency | Low | Higher — reader applies deletes on the fly |
| Use when | Rare updates, read-heavy analytics | Frequent upserts, CDC, streaming |

Ask the user whether the table has row-level mutations before defaulting; if it does, MoR + v2 is usually the right pick.

### 2. Compaction

Iceberg compaction is a **procedure call**, not a background process. Nothing auto-compacts unless the user runs one of:

```sql
-- Bin-pack or sort small files, apply pending deletes (for MoR)
CALL catalog.system.rewrite_data_files(
  table   => 'ns.events',
  strategy => 'binpack',
  options => map('min-input-files','5','target-file-size-bytes','536870912')
);

-- Compact the manifest list (speeds up scan planning)
CALL catalog.system.rewrite_manifests('ns.events');
```

**Where it runs:** any Spark session with `iceberg-spark-runtime` and catalog access. Typical deployment patterns:

- **Airflow / Dagster DAG** — scheduled `rewrite_data_files` every N hours
- **Streaming job post-hook** — compact after each micro-batch commit window
- **Managed catalog** — Tabular, Snowflake Polaris, AWS Glue and Dremio can run compaction for you; no user job needed
- **Ad-hoc cron** — plain `spark-submit` on a schedule

Compaction does not block writers — it creates a new snapshot and the old files remain until expiration.

### 3. Snapshot expiration & orphan cleanup

Iceberg keeps old snapshots forever unless told otherwise. Two calls every user should schedule (usually together with `rewrite_data_files`):

```sql
CALL catalog.system.expire_snapshots(
  table => 'ns.events',
  older_than => TIMESTAMP '2026-04-01 00:00:00'
);

CALL catalog.system.remove_orphan_files(table => 'ns.events');
```

Without this the catalog grows unboundedly and old data files never get GC'd.

**Note:** none of these procedures are written by the skill. They are a deployment concern — mention them after the rewrite is green, and let the user wire them into whatever scheduler they already use.

## Path schemes

The mapping resolver is URI-aware. Sub-scheme variants of the same storage are treated as equivalent:

| Canonical scheme | Aliases | Example |
|---|---|---|
| `s3` | `s3a`, `s3n` | `s3://bucket/key` |
| `hdfs` | `webhdfs` | `hdfs://nameservice/path` |
| `abfs` | `abfss` | `abfs://container@account.dfs.core.windows.net/path` |
| `gs` | — | `gs://bucket/key` |
| `viewfs` | — (kept distinct from `hdfs`) | `viewfs://nameservice/path` |
| `file` | bare paths (`/tmp/...`, `./data/...`) | `file:///tmp/x` |

A mapping entry `s3://bucket/users/*` matches paths in the code regardless of whether the code writes `s3://`, `s3a://`, or `s3n://`. The same is true for `hdfs`/`webhdfs` and `abfs`/`abfss`.

**`path_arg` in the worklist is preserved verbatim** — the equivalence is applied only when comparing against mapping globs.

### Glob syntax

Mapping patterns support shell-style globs in the authority and path components:

- `*` matches any character sequence (including `/`) — same as `s3://bucket/users/*` matching `s3://bucket/users/2024/01/data.parquet`
- `**` is provided for clarity and behaves the same as `*` in single-pattern matches
- `?` matches one character
- Brackets like `[abc]` are not supported

This matches the convention used by `aws s3` and similar cloud-storage tools.

### Bare local paths

Bare paths (`./data/x`, `/tmp/fixtures/x`) are treated as `file://` for matching. Relative paths are resolved against the project root passed via the CLI.

### viewfs limitation

`viewfs://` is **not** auto-resolved against an underlying `hdfs://` cluster. Mount-point resolution depends on cluster config we do not read. If your project uses both `viewfs://` and `hdfs://` (e.g., logical mount in jobs, physical path in DDL), list both schemes explicitly in the mapping:

```json
{
  "tables": [
    { "path_glob": "viewfs://nameservice/data/users/*", "namespace": "analytics", "table": "users" },
    { "path_glob": "hdfs://realCluster/data/users/*",   "namespace": "analytics", "table": "users" }
  ]
}
```

### Scheme-less globs (backward compat)

Glob patterns without a scheme and not starting with `/` (e.g., `*users*`, `data/*`) fall back to `fnmatch` against the **full raw `path_arg` string** — including any scheme prefix. So `*users*` matches `s3://bucket/users/x` because the full raw string contains the literal substring `users`. This preserves legacy mapping files that pre-date URI awareness. New patterns should prefer explicit schemes for clarity.

### Unknown schemes

Unknown schemes (e.g., `ftp://`, custom enterprise schemes) emit a one-time stderr warning per scheme. They participate in matching only via exact `raw_scheme` equality — no aliasing applied.

## Constant folding

The detector resolves name-to-literal bindings at the file level so that I/O calls using a named constant become as informative as those using a literal directly.

```python
EVENTS_PATH = "s3://bucket/events"
df = pd.read_parquet(EVENTS_PATH)   # path_arg = "s3://bucket/events"
```

### What is resolved

- **Python:** module-level `X = "..."`, function-local `X = "..."`, one level of `+` concat (`X = BASE + "/events"` when `BASE` is already a known literal).
- **Java:** class `static final` and inline-initialised `final` fields, method-local `final` variables, one level of `+` concat across fields.
- **Scala:** object/class-level `val`, def-local `val`, one level of `+` concat.

### What is skipped

- **f-strings** (`f"..."`, `s"..."` Scala) — interpolation depends on runtime values.
- **`.format()`, `%`-format, multiplication** — only `+` concat is recognised.
- **Reassignment** — any reassigned name is marked unresolvable. `attrs.skipped_reason = "reassigned"` records the reason.
- **Constructor-only Java fields** — `private final String x;` initialised inside a constructor is not parsed.
- **Cross-file references** — `from config import X` is not followed; constants in other files are out of scope.
- **3+ operand concat** — `A + B + C` is not resolved; only single-`+` expressions.
- **Non-literal RHS** — `os.getenv(...)`, function calls, etc.

### Audit trail

When a match resolves a name, `match.attrs["resolved_from"] = "NAME@file:line"` is set so the worklist preserves the original source location. The detector emits the same `path_arg` value regardless of whether it came from a literal at the call site or a resolved binding.

## Partition spec extraction

When a Spark write site uses `.partitionBy(...)` or `.bucketBy(N, ...)`, the detector captures the partition specification structurally and propagates it into the worklist so the migration agent can generate the correct Iceberg `PartitionSpec` per target.

### Supported transforms (MVP)

| Spark API | Iceberg transform |
|---|---|
| `.partitionBy("col")` | `identity(col)` |
| `.partitionBy("col1", "col2", ...)` | one `identity(col)` per arg |
| `.bucketBy(N, "col1", "col2", ...)` | one `bucket(N, col)` per col arg |

Time-based transforms (`year`/`month`/`day`/`hour`) and `truncate(N, col)` are out of MVP scope — Spark uses these through `withColumn(...)` pre-pass + identity partitioning, which would require intra-method data flow.

### Detection scope

- **Python PySpark** — `df.write.partitionBy/.bucketBy`
- **JVM Spark** — Java/Scala DataFrame API
- **Hive DDL in `.sql` files** — `PARTITIONED BY (col1, col2)` (identity transforms only; Hive `PARTITIONED BY` has no `bucket(N)` form)

Constants used as args resolve via the const-folding module (1.1): `REGION = "region"; df.write.partitionBy(REGION)` resolves to `identity(region)`.

### Edge cases

- `partitionBy()` (no args) → no transforms
- `partitionBy(*cols)` (splat) → skipped silently
- `partitionBy(year("ts"))` (function call) → skipped silently (out of scope)
- `bucketBy(N_var, "col")` where N is an identifier → skipped (MVP requires int literal for N)
- Multiple `partitionBy` calls in one chain → merged in chain order

### Worklist output

Write-direction entries gain an optional `partition_spec` array:

```json
{
  "site": "src/jobs/users.py:54",
  "kind": "write",
  "target": "analytics.users",
  "partition_spec": [
    {"kind": "identity", "column": "region"},
    {"kind": "bucket", "column": "uid", "n": 8}
  ]
}
```

When no partitions are detected, the field is **omitted** from the JSON (not `[]`).

### Code↔DDL mismatch detection

If both `df.write.partitionBy(...).saveAsTable(t)` (code) and `CREATE TABLE t ... PARTITIONED BY (...)` (DDL in `.sql` file) exist for the same table, the analyzer compares both `partition_spec` values. If they diverge, the match's `attrs["partition_mismatch"]` is populated:

```
code: identity(region); ddl: identity(date_col)
```

The agent sees this warning and can flag it before applying the rewrite.

## Dynamic SQL loading

The detector finds call-sites that load `.sql` files at runtime and cross-references them with parquet/orc tables defined in those (or related) files. This catches the common pattern where SQL is stored separately from code:

```python
sql = open("queries/events_update.sql").read()
spark.sql(sql)
```

### Detected patterns

| Language | Pattern | Example |
|---|---|---|
| Python | `py_open` | `open("x.sql")` |
| Python | `py_path_read_text` | `Path("x.sql").read_text()` |
| Python | `py_pkgutil_get_data` | `pkgutil.get_data(__name__, "x.sql")` |
| Java | `java_files_read` | `Files.readAllBytes(Path.of("x.sql"))` |
| Java/Scala | `java_resource_stream` | `getClass().getResourceAsStream("/sql/x.sql")` |

### What's parsed in the loaded SQL

Beyond `CREATE TABLE ... STORED AS PARQUET` (already handled), the SQL registry now also extracts non-DDL references:

- `INSERT INTO <table>` and `INSERT OVERWRITE TABLE <table>` → write reference
- `UPDATE <table> SET ...` → write reference
- `MERGE INTO <table>` → write reference
- `FROM <table>` / `JOIN <table>` → read reference

CTE names introduced by `WITH <name> AS (...)` are skipped from `FROM`/`JOIN` references.

### Cross-reference behavior

For each loader, the loader's `sql_filename` is resolved against the project tree in three steps:

1. Path relative to the loader's containing file's directory.
2. Path relative to project root.
3. Basename match across all registered `.sql` files (with `match_kind="basename_unique"` or `"basename_ambiguous"`).

Tables mentioned in the resolved SQL file (via CREATE / INSERT / FROM / JOIN / etc.) are joined against the registry of parquet/orc `CREATE TABLE` definitions across all `.sql` files. This handles the common pattern of `schema.sql` (with CREATE) and `queries/*.sql` (with INSERT only).

### Worklist output

Each cross-reference appears in `lakehouse-worklist.json` under `dynamic_sql_loaders`:

```json
{
  "file": "src/jobs/events.py",
  "line": 42,
  "pattern": "py_open",
  "sql_filename": "queries/events_update.sql",
  "confidence": "high",
  "resolved_to": "queries/events_update.sql",
  "match_kind": "exact_path",
  "tables": [
    {"name": "events", "format": "parquet", "ddl_file": "schema.sql", "ddl_line": 8}
  ]
}
```

### Limitations

- SQL templating (Jinja, `.format`, `${...}`) is not parsed.
- In-SQL `\i` / `SOURCE` / `!include` directives are not followed.
- ORM-generated SQL (SQLAlchemy `select(...)`, jOOQ DSL) is not detected.
- Dynamic table names within SQL (`INSERT INTO {schema}.events`) are not resolved.

## Preview before writes (the markdown-only equivalent of "dry run")

In the parent Python skill this was a `--dry-run` CLI flag that printed a Summary / Worklist / Prepass-diff / Build-file-updates preview without touching the project. In the markdown-only skill the equivalent is **the migration plan you present to the user before applying changes** (see `SKILL.md` "Present the migration plan and wait for approval"). The plan should contain the same information the dry-run output contained: the table of detected call sites, the per-table target `(namespace, table)` mapping, the proposed pyiceberg / iceberg-spark-runtime additions to build files, and any pipeline anti-patterns surfaced for user decision.

Don't modify any file until the user replies `go` (or specifies changes). That's the whole "dry-run" — it's a property of the workflow, not a flag.

## Phased migration runbook

For each target Iceberg table in the migration plan, the agent should compose (by hand, in the markdown-only skill) a per-table directory under `iceberg-runbook/` containing:

- `migration-plan.md` — phase descriptions, pre-flight checklist, code-sites table, warnings
- `phase1_add_files.sql` — Spark SQL for `system.add_files` (in-place metadata creation)
- `phase2_rewrite.sql` — Spark SQL for `system.rewrite_data_files` (compaction)
- `phase3_switchover.sql` — three OPTION blocks: Spark VIEW, HMS direct rename, Application-level rename per worklist

Plus `iceberg-runbook/README.md` as a top-level index with a summary table of all migrations.

### Phases

| Phase | Estimated runtime | Risk |
|---|---|---|
| 1: add_files | minutes | Low — reversible by dropping target table |
| 2: rewrite_data_files | hours | Medium — resource-heavy, can be deferred |
| 3: switchover | minutes | Coordinated cutover — requires consumer alignment |

### Phase 3 options

Phase 3 has three switchover patterns. The user picks ONE per stack and comments out the others:

- **OPTION A (Spark VIEW)** — replace old Hive table with a view pointing at Iceberg. Works for SELECT-only consumers; breaks `INSERT INTO` clients.
- **OPTION B (HMS direct rename)** — atomic at metastore level. Requires admin access.
- **OPTION C (Application-level rename)** — update each call site in the code per `lakehouse-worklist.json`. Listed in the SQL file as comments.

### Limitations

- Spark SQL syntax only (no Trino, ClickHouse, Snowflake).
- Schema in `phase1_add_files.sql` is a placeholder — the operator must run `spark.read.parquet(...).printSchema()` and paste the result.
- No data-size estimation or runtime prediction.
- No DAG between tables (each migration is independent).
- `partition_mismatch` warning is included in the runbook, but the operator must decide which side (code or DDL) is correct.

## Iceberg-native pipeline optimizations

Iceberg removes the need for several Parquet-era pipeline patterns. When migrating, look for these and propose alternatives — see the mandatory analysis pass in [SKILL.md](./SKILL.md). This section is the deep dive: full before/after SQL, detection logic, and migration caveats.

### Pattern 1: Increment computation via snapshot diff

**Setup.** A daily job lands a full table snapshot under partition `ctl_loading=<run_date>`. A separate SQL (`t_<x>_inc.sql`) joins today's partition with yesterday's to derive the row-level delta — used downstream to emit change events to Kafka / downstream marts.

**Before (Parquet):**

```sql
-- t_agr_bond_inc.sql (parquet era)
WITH today AS (
  SELECT * FROM agr_bond WHERE ctl_loading = ${today}
), yesterday AS (
  SELECT * FROM agr_bond WHERE ctl_loading = ${yesterday}
)
SELECT
  CASE WHEN y.id IS NULL THEN 'INSERT'
       WHEN t.id IS NULL THEN 'DELETE'
       WHEN t.attr1 <> y.attr1 OR t.attr2 <> y.attr2 OR ... THEN 'UPDATE'
  END AS op,
  COALESCE(t.id, y.id) AS id,
  t.attr1, t.attr2, ...
FROM today t
FULL OUTER JOIN yesterday y ON t.id = y.id
WHERE NOT (y.id IS NOT NULL AND t.id IS NOT NULL
           AND t.attr1 = y.attr1 AND t.attr2 = y.attr2 AND ...);
```

**After (Iceberg) — two valid replacements depending on intent:**

**(a) Maintain the target table from a source-of-truth dataset → `MERGE INTO`:**

```sql
MERGE INTO {{datamart_name}}.t_agr_bond AS tgt
USING (
  SELECT * FROM source_of_truth WHERE ctl_loading = ${today}
) AS src
ON tgt.id = src.id
WHEN NOT MATCHED THEN
  INSERT *
WHEN MATCHED AND (
       src.attr1 <> tgt.attr1
    OR src.attr2 <> tgt.attr2
    OR ... -- compare every business column; null-safe operator may be needed for nullable cols
  ) THEN UPDATE SET *
WHEN NOT MATCHED BY SOURCE THEN
  DELETE;
```

Requires `format-version=2` and (for low-latency) `write.merge.mode=merge-on-read`. See [reference.md § Post-Migration Operational Concerns](#post-migration-operational-concerns).

**(b) Emit change events for downstream → `table.changes` (changelog scan):**

```sql
-- Read the changelog produced by writes between two snapshots:
SELECT _change_type, _change_ordinal, _commit_snapshot_id, *
FROM {{datamart_name}}.t_agr_bond.changes
FOR SYSTEM_VERSION AS OF FROM (snapshot_yesterday) TO (snapshot_today)
ORDER BY _change_ordinal;
```

`_change_type` values: `INSERT`, `DELETE`, `UPDATE_BEFORE`, `UPDATE_AFTER`. An update produces two rows (BEFORE + AFTER) with the same `_change_ordinal`. No diff query needed — Iceberg tracks this at the metadata layer.

**Caveats:**
- `MERGE` requires unique keys on the join condition; if `id` is not unique in the source you must dedupe in the `USING` subquery.
- Comparing nullable columns with `<>` returns `NULL` (treated as false). Use `IS DISTINCT FROM` or `NVL`-pad to detect changes correctly.
- `table.changes` is meaningful only AFTER the table is written via MERGE / append / overwrite from the source — it does not retro-fill history. Plan a one-time backfill if downstream needs historical change events.
- If the target has retention shorter than the downstream consumer's lag, `changes` will gap. Tune `expire_snapshots` accordingly (see [ICEBERG_WF_GUIDE.md](./ICEBERG_WF_GUIDE.md)).

### Pattern 2: Manual tombstone columns

**Before:** Tables carry an `is_deleted BOOLEAN` or `deleted_at TIMESTAMP` column because Parquet has no row-level delete; downstream filters add `WHERE NOT is_deleted` to every query.

**After:** Drop the tombstone column. Use ordinary `DELETE FROM tgt WHERE ...` against the Iceberg table. Set `format-version=2` and `write.delete.mode=merge-on-read` so deletes are cheap.

**Caveat:** Existing downstream queries that filter on `is_deleted` need to be rewritten — this is breakage, not just an internal change. Propose the alternative to the user; do not silently drop the column.

### Pattern 3: Full-partition rewrite for late-arriving data

**Before:** Late-arriving rows for `part_dt='2026-04-15'` arrive on `2026-05-01`. The Parquet pipeline rewrites the entire `part_dt='2026-04-15'` partition (read all, append new, write back) because there is no row-level update.

**After:** `MERGE INTO ... ON tgt.id = src.id AND tgt.part_dt = src.part_dt` — Iceberg writes only the affected data files within that partition; the rest of the partition is untouched.

### Pattern 4: Custom CDC / changelog tables

**Before:** A `t_<x>_changelog` table populated by triggers / batch scripts to record what changed in the main table, used for CDC to Kafka.

**After:** The main Iceberg table itself IS the CDC source via `table.changes`. Retire the changelog table and rewrite the Kafka emitter to read from `.changes`.

### Detection cheat-sheet

The migrator (or the agent at audit time) should flag for review:

| Signal | Likely pattern |
|---|---|
| SQL file named `*_inc.sql`, `*_diff*.sql`, `*_delta*.sql`, `*_changes*.sql` | Pattern 1 (increment via diff) |
| `FULL OUTER JOIN ... ON .id = .id` between two filtered slices of the same table | Pattern 1 |
| `LEFT JOIN ... WHERE rhs.id IS NULL` between two snapshots | Pattern 1 (insert detection half) |
| `EXCEPT` / `MINUS` between filtered slices of the same source | Pattern 1 |
| `WITH today AS (...), yesterday AS (...)` CTE structure | Pattern 1 |
| Column named `is_deleted` / `deleted_at` / `tombstone` / `is_active`+`valid_to` | Pattern 2 |
| Repeated full-partition `INSERT OVERWRITE TABLE t PARTITION (part_dt=...)` for the same partition | Pattern 3 |
| Table name ending `_changelog` / `_cdc` / `_history` with a foreign key to the main table | Pattern 4 |

These are **signals**, not rules. A `_inc.sql` may legitimately compute a metric increment, not a row-level diff. Surface the finding to the user and confirm intent before proposing the rewrite.

## Known Limitations

- **FQN propagation on read sites** — after rewriting `saveAsTable("Foo")` → `writeTo("ns.Foo")`, every downstream reference to the bare name (`spark.table("Foo")`, `CACHE TABLE Foo`, `SELECT ... FROM Foo`, `DROP TABLE Foo`) must be updated to the fully-qualified `ns.Foo`. Otherwise those calls resolve through the default session catalog and miss the Iceberg table entirely. The detector reports these reads, but the worklist does not automatically bind them to the write-site rewrite — walk each `spark_read_table` / SparkSQL match in the same file and rename by hand.
- **Partitioning and bucketing** — `partitionBy(...)` / `bucketBy(...)` on a write is preserved as a `TODO(iceberg)` comment, never auto-rewritten. If the original table was partitioned or bucketed, **ask the user** in Step 3 whether the layout matters, then pre-create the Iceberg table with the matching partition spec (`PARTITIONED BY (bucket(N, col))` / `PARTITIONED BY (days(col))`) before letting the rewrite run. Using `createOrReplace()` on a saveAsTable with bucketing silently drops the spec.
- **Structured Streaming** (readStream/writeStream with parquet/orc sinks) is **detected but not rewritten** — the transformer inserts a `TODO(iceberg)` comment. Migrate manually using `.format("iceberg")` + `writeStream.toTable("ns.t")` / `.option("path", ...)`.
- **pyarrow dataset API** (`ParquetFile`, `ParquetDataset`, `pa.dataset.*`) is also warn-only — rewrite to `catalog.load_table(...).scan().to_arrow()` by hand.
- **Cloud catalog configs** (Glue, Nessie, REST) need manual setup — this tool generates SQLite dev config for Python, and leaves JVM catalog config untouched
- **Hive table data migration** — the tool rewrites *code* but does not migrate existing parquet/ORC data; use `CALL system.migrate(...)` for in-place migration or `CTAS` for copy
- **Scala 2.13 / Spark 3.4** — the generated Maven/Gradle coordinates target Spark 3.5 + Scala 2.12; adjust manually for other versions
- **`LOCATION` clause** on `CREATE EXTERNAL TABLE` — rewritten to `USING iceberg LOCATION ...` but Iceberg's LOCATION semantics differ from Hive's; review manually.
- **INSERT INTO / OVERWRITE** — detected but intentionally not rewritten (same SQL works on Iceberg tables). Reported only so you know the file is touching table data.
- **False positives** — `INSERT INTO`, `saveAsTable`, and `USING parquet` regexes match *any* table by that pattern, not just the table you're migrating. Use `filter_matches` to scope if a project touches multiple tables.
