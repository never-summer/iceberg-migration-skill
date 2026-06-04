---
name: iceberg-migration-skill
description: Convert Parquet/ORC read/write to Apache Iceberg in Python, Java, or Scala projects. Use when the user says "convert parquet", "migrate to iceberg", "parquet to iceberg", "migrate hive to iceberg", "convert orc", "migrate orc to iceberg", or asks to move Hive-parquet tables to Iceberg.
---
# Parquet/ORC → Iceberg Conversion Skill

**Announce at start (verbatim, two lines):**

> "I'm using the iceberg-migration-skill to convert this project."
> "I will first do a read-only reconnaissance pass (read the four guides, scan for I/O sites + pipeline anti-patterns, locate any existing maintenance wf and Iceberg conf), then present a migration plan and wait for your `go` before modifying files."

## Recommended order (skip only with good reason)

These steps are recommendations, not enforced gates. They exist because past runs that skipped them produced shallow mechanical Parquet→Iceberg rewrites and missed the MERGE / `.changes` / MoR opportunities the user actually wants.

1. **Read the four guides first** — [S2T_GUIDE.md](./S2T_GUIDE.md), [ICEBERG_WF_GUIDE.md](./ICEBERG_WF_GUIDE.md), [reference.md](./reference.md), [examples.md](./examples.md). They contain project-shape assumptions the rest of this skill relies on. Reading DDLs / `pom.xml` / `ctl.yml` before these is fine if it helps, but the guides should be on your context by the time you write the migration plan.
2. **Do reconnaissance before proposing changes** — surface I/O sites, scan for pipeline anti-patterns, locate any existing maintenance wf, locate any existing Iceberg Spark conf YAML parameter. Concrete commands are below in the "Reconnaissance" section.
3. **Present a migration plan and wait for the user's `go`** before editing files. The plan template lower in this document is a useful structure; deviate when it doesn't fit the project.

If you catch yourself printing an "🔍 Анализ" / "Tables summary" block before the user has the plan in front of them, you skipped step 3 — back up.

## Suggested workflow — Reconnaissance → Plan → Apply

The three sections below describe a workflow that works. You are not required to execute them in lockstep, but the order — reconnaissance, then a plan, then writes — is what gives the user a chance to redirect you before files change. Past runs that skipped straight to mechanical Parquet→Iceberg rewrites missed the MERGE / `.changes` / MoR opportunities the user is paying you to find. Deviate when it helps; don't deviate to save time.

### Reconnaissance (read-only — no writes)

Work through these — they build on each other; skipping one usually means the migration plan you produce later is incomplete:

1. **Read the mandatory guides** in this skill directory:

   - [S2T_GUIDE.md](./S2T_GUIDE.md) — for table-spec source-of-truth in Hadoop/Spark datamart projects
   - [ICEBERG_WF_GUIDE.md](./ICEBERG_WF_GUIDE.md) — for wf/ctl, compaction, Spark conf
   - Skim [examples.md](./examples.md) and [reference.md](./reference.md) for patterns you'll need
2. **Surface I/O sites** with native search. These commands find Parquet/ORC reads, writes, and Hive DDLs — they will produce false positives (matches inside strings, comments). Inspect each match yourself; do not trust hit-counts.

   ```bash
   # Spark / pandas / pyarrow read+write sites in source code:
   rg -n --type-add 'src:*.{py,scala,java}' -tsrc \
      'parquet|\.format\("parquet"\)|saveAsTable|STORED AS PARQUET|USING parquet|\borc\b|STORED AS ORC|USING orc' \
      <project>/src 2>/dev/null

   # Hive DDL files:
   grep -rn -E 'STORED AS (PARQUET|ORC)|USING (parquet|orc)' \
      <project>/src/main/resources/sql/ddl/ 2>/dev/null

   # Generic write paths:
   rg -n 'write\.(parquet|orc|format)|insertInto|saveAsTable' <project>/src 2>/dev/null
   ```

   This is intentionally noisier than the old Python detector — the detector parsed AST and resolved string-formatted paths. With raw search you may catch a `parquet` mentioned in a comment or a log string; read each hit before adding it to the worklist.
3. **Pipeline anti-pattern scan** — explicit commands (run each, save findings for the Phase B report):

   ```bash
   # Increment / diff / changelog SQL (-> MERGE / .changes candidates):
   find <project> -type f \( -name "*_inc.sql" -o -name "*_diff*.sql" -o -name "*_changes*.sql" -o -name "*_delta*.sql" \)

   # FULL OUTER JOIN between snapshots (-> MERGE candidates):
   grep -rn "FULL OUTER JOIN\|full outer join" <project>/src/main/resources/sql/

   # EXCEPT / MINUS between filtered slices (-> MERGE candidates):
   grep -rn -i "\bEXCEPT\b\|\bMINUS\b" <project>/src/main/resources/sql/

   # Tombstone columns (-> MoR + native DELETE candidates):
   grep -rn -i "is_deleted\|deleted_at\|is_active.*valid_to" <project>/src/main/resources/sql/ddl/

   # Custom changelog/CDC tables (-> table.changes candidates):
   find <project> -type f -name "*_changelog*.sql" -o -name "*_cdc*.sql" -o -name "*_history*.sql"
   ```

   Full pattern catalogue + caveats: [reference.md § Iceberg-native pipeline optimizations](./reference.md#iceberg-native-pipeline-optimizations).
4. **Locate existing maintenance wf** (do NOT invent a name yet):

   ```bash
   grep -rn 'rewrite_data_files\|expire_snapshots\|exp_iceberg\|hdfs_care' \
       <project>/src/main/resources/wf/ctl/ <project>/src/main/resources/sql/dml/
   ```

   Record what you found (or "nothing — will propose in Phase B").
5. **Locate existing Iceberg conf YAML parameter** (do NOT invent a name yet):

   ```bash
   grep -rn 'spark.sql.extensions.*IcebergSparkSessionExtensions\|SparkSessionCatalog' \
       <project>/src/main/resources/wf/ <project>/src/main/resources/devops/ \
       <project>/src/main/resources/mart*.yml
   ```

   Record what you found.
6. **Locate S2T inputs** — `<project>/src/main/resources/s2t/s2t.xlsx` (or `hadoop_S2T_*.xlsx`), `<project>/src/main/resources/devops/devops.json`, `<project>/src/main/resources/wf/ctl/1_ctl_entities.yml`. Pull `datamart_name`, ТУЗ, yarn queue, per-table `entity_id`. If S2T Excel is unreadable, ask the user — do not invent.
7. **Sibling project Iceberg workflow recon** — find sibling datamart projects in the monorepo that have **already migrated to Iceberg**. Their workflows are the gold-standard reference template — much more useful than the generic templates in `ICEBERG_WF_GUIDE.md`, which describe a structure but cannot capture project-specific naming, parameter sets, lock conventions, schedule cron, or class-path patterns. Do NOT generate a wf from the guide alone — always anchor it to a real working example.

   ```bash
   # Step 7.1: list all ctl/ directories in the monorepo (excluding the current project):
   find <repo-root> -type d -name ctl -not -path "*/<current_project>/*"

   # Step 7.2: within those, find ctl yaml/properties files that already mention Iceberg —
   # these are the projects that have done this migration before:
   grep -rln -i "iceberg\|IcebergSparkSessionExtensions\|rewrite_data_files\|expire_snapshots\|exp_iceberg\|upd_iceberg\|hdfs_care" \
       <ctl_dirs_from_7.1>
   ```

   If matches found:

   - Pick 1–2 sibling projects closest to the current one (similar data domain, similar size, similar layer pattern — AUX / HIST / AL / etc.).
   - For one Iceberg table in the chosen sibling: **read the full wf definition** plus its linked DML scripts (`exp_iceberg_*.sql`, `upd_iceberg_*.sql`), the relevant `mart.yml` keys it references, the `1_ctl_entities.yml` entry, and the `devops.json` entry. Trace end-to-end so you know how the pieces fit.
   - In Phase B, record: **"Reference template: `<sibling_project>/wf/ctl/<file>.yml:<line-range>` for table `<t>`"** — and use that as the structural template for your wf in the current project. Adapt names, IDs, paths — preserve structure, parameter ordering, naming convention, lock pattern.

   If no matches found:

   - State this in Phase B: `"No sibling Iceberg workflow found in monorepo — falling back to generic templates in ICEBERG_WF_GUIDE.md"`.
   - Then use ICEBERG_WF_GUIDE templates as fallback. Flag this to the user — the resulting wf has no project-specific anchor and is more likely to need iteration.

### Present the migration plan and wait for approval

After reconnaissance, output **one structured plan** to the user using the template below, then **stop and wait for explicit approval**. Do not start writing files.

The plan opens with a reconnaissance self-check — it forces you to declare what you actually did vs. skipped. An honest `n` on a row is a signal to loop back and fill it in before the user sees the plan, not a hard stop — and if the user has explicitly told you to skip a row (e.g. "this project has no S2T"), that's fine too.

```
## Phase A findings + proposed plan

### Reconnaissance self-check (verify you covered these)
- [ ] Read S2T_GUIDE.md                                                    y/n
- [ ] Read ICEBERG_WF_GUIDE.md                                             y/n
- [ ] Read reference.md                                                    y/n
- [ ] Ran I/O site search (rg/grep from Reconnaissance step 2 — paste     y/n
      hit count for each command or "no matches")
- [ ] Ran all 5 anti-pattern greps (paste each command + first line of     y/n
      its output OR "no matches"; the 5 commands are: _inc.sql find,
      FULL OUTER JOIN grep, EXCEPT/MINUS grep, tombstone grep, *_changelog find)
- [ ] grep'ed for existing maintenance wf (paste command + result)         y/n
- [ ] grep'ed for existing iceberg conf YAML param (paste command +        y/n
      result)
- [ ] Located S2T inputs (s2t.xlsx, devops.json, 1_ctl_entities.yml)       y/n
- [ ] Sibling project Iceberg ctl recon (paste find + grep; or "no            y/n
      sibling Iceberg ctl found in monorepo"). If found, name the
      sibling project + file you will use as reference template.

If any item is `n`, consider whether you should fill it in before presenting the plan — it likely affects the recommendation quality. The user can also tell you to skip an item explicitly (e.g. "this project has no S2T, skip that row").

### Reference template (from Phase A.7 sibling recon)
- Reference: `<sibling_project>/wf/ctl/<file>.yml:<line-range>` for table `<t>`
- (or: "No sibling Iceberg ctl found — falling back to ICEBERG_WF_GUIDE generic template (less anchored, may need iteration)")

### Tables to migrate (from worklist + S2T)
| # | Source | Target Iceberg | Format | Code sites | DDL file | entity_id | Mode (MoR/CoW) | Compaction template |
|---|---|---|---|---|---|---|---|---|
| 1 | ... | ns.t | parquet | 7 | ddl/x.sql | 920... | MoR | MoR-aware (see ICEBERG_WF_GUIDE "Полный цикл обслуживания для MoR") |
...

**Mode selection — user input is ground truth.**

If the user has explicitly named a mode (`CoW`, `Copy-on-Write`, `MoR`, `merge-on-read`) for the migration, generate DDL with **that** mode for the affected tables. Do **not** override based on the DML heuristic. If you believe the user's choice is operationally risky (e.g. MoR without a MoR-aware compaction template, or CoW under heavy row-level mutations causing rewrite amplification) — flag the concern in the plan and **ask**, do not silently switch.

**Default mode (used only when the user has not specified):** MoR if the table has any `UPDATE` / `DELETE` / `MERGE INTO` in its DML scripts (`grep -ril 'UPDATE \|DELETE FROM\|MERGE INTO' src/main/resources/sql/dml/`), otherwise CoW. State the basis for each row in the table comment — either "user-specified" or which DML hits triggered the default.

**Compaction template rule:** MoR tables MUST use the MoR-aware template (`delete-file-threshold` + `rewrite_position_delete_files` + `rewrite_manifests`). CoW tables use the basic `rewrite_data_files` template. Using the CoW template on a MoR table is a silent footgun: delete files accumulate, scans get slower over time, and you do not notice until production degrades.

### Pipeline anti-patterns detected (Iceberg-native proposals — NEEDS APPROVAL)
1. `path/to/t_X_inc.sql:1` — FULL OUTER JOIN today/yesterday → propose `MERGE INTO` (see reference.md §Iceberg-native pipeline optimizations Pattern 1).
   ALSO propose: retire `wf_X_inc` workflow entry once MERGE replaces the diff.
2. (or: "no anti-patterns found")
...

### Existing infrastructure found (Phase A grep results)
- Maintenance wf: `<name>` at `wf/ctl/<file>.yml:<line>` — will REUSE.
  (or: "not found — propose name: wf_<table>_service / wf_schema_hdfs_care / wf_iceberg_maintenance — pick one")
- Iceberg conf YAML param: `{{mart.<name>}}` at `mart.yml:<line>` — will REUSE.
  (or: "not found — propose name: spark_iceberg / iceberg_conf / spark_submit_cmd_iceberg_service — pick one")

### Files I plan to change (or create) in Phase C
- mart.yml — define `<conf_param_name>` (if not reusing)
- ctl.yml — add `<wf_name>` per-table maintenance entry, patch existing wf reading these tables to add `{{mart.<conf_param_name>}}`
- sql/ddl/<layer>/<table>.sql — switch STORED AS PARQUET → USING iceberg + TBLPROPERTIES
- sql/dml/exp_iceberg_<table>.sql / upd_iceberg_<table>.sql — new compaction scripts
- s2t/qaapi/*.feature — add Gherkin DDL scenarios for new tables
- (If anti-pattern accepted) sql/dml/<table>_inc.sql — replace with MERGE INTO + `.changes` consumer

### Open questions for the user (BLOCK on these)
- MoR vs CoW per table? If you've already specified a mode, that's what I'll use. Otherwise the default applies: MoR if the table has UPDATE/DELETE/MERGE in its DML, CoW otherwise.
- Accept the MERGE / `.changes` rewrite for `t_X_inc.sql`? (Y/N)
- Maintenance wf name to use (if not reusing)?
- Iceberg conf YAML param name (if not reusing)?

---
Reply **"go"** to apply this plan as-is. Or list changes/exclusions.
```

### Apply changes (writes) — only after explicit user approval

Execute in this fixed order so reviews stay sane:

1. `mart.yml` — define / reuse the iceberg conf YAML param.
2. `wf/ctl/*.yml` — maintenance wf + patches to existing wf (`{{mart.<conf>}}` reference, no inlining).
3. `sql/ddl/<layer>/<table>.sql` — switch to `USING iceberg` + TBLPROPERTIES per S2T.
4. `sql/dml/exp_iceberg_<table>.sql` and `upd_iceberg_<table>.sql` — compaction scripts per ICEBERG_WF_GUIDE templates.
5. `s2t/qaapi/*.feature` — Gherkin scenarios.
6. (If approved) Replace `*_inc.sql` with MERGE INTO + retire `wf_*_inc`.
7. (Optional) Write a small `iceberg-runbook/` directory by hand: one `phase1_add_files.sql` and one `phase2_rewrite.sql` per migrated table, following the templates in [reference.md § Phased migration runbook](./reference.md#phased-migration-runbook). This used to be auto-generated; in the markdown-only skill you compose them from the templates and the per-table info already captured in the plan.

After applying: print a final summary (what was changed, what was created, what was retired) and remind the user to re-run the `rg`/`grep` recon commands from the Reconnaissance section to verify zero residual `STORED AS PARQUET` / `USING parquet` references.

---

The three MANDATORY blocks below detail the rules for guides, conf, and pipeline analysis. The suggested workflow above is the operational discipline that makes those rules actually apply.

## ⚠ MANDATORY — read these two guides before doing anything

**Before Step 1, you MUST read both of these files. They ship with this skill — same directory as this `SKILL.md`. They override every default below when they conflict, and ignoring them will produce a broken migration.**

- 📖 **[S2T_GUIDE.md](./S2T_GUIDE.md)** — Source-to-Target spec system used in Hadoop/Spark datamart projects. Authoritative for:

  - Table specs (sheets `Tables`, `Columns`, `Partitions`, `Indexes`, `Constraints` in `s2t.xlsx` / `hadoop_S2T_*.xlsx`)
  - Column metadata (names, types, nullability, descriptions)
  - DDL is **generated** from S2T, not from existing parquet. Pull schema from `src/main/resources/s2t/s2t.xlsx` (or `hadoop_S2T_<PROJECT>_v<n>.xlsx`), not from `pq.read_schema`.
  - Gherkin feature files (`ift.feature`, `st_skl.feature`) under `src/main/resources/s2t/qaapi/` must be updated with the new table scenarios. ТУЗ (`u_<id>`), очередь yarn, `datamart_name`, `entity_id` come from S2T + `devops.json` + `1_ctl_entities.yml`.
- 📖 **[ICEBERG_WF_GUIDE.md](./ICEBERG_WF_GUIDE.md)** — Oozie-based `wf/ctl/*.yml` workflow system. Authoritative for:

  - Required Spark conf for Iceberg (`spark.sql.extensions=...IcebergSparkSessionExtensions`, `spark_catalog.type=hive`, `rewrite.partial-progress.enabled=true`, etc.) — see "Spark-конфигурация для Iceberg" section.
  - Compaction / expire_snapshots / remove_orphan_files run via the project's **maintenance wf**, NOT via standalone Spark jobs. **Locate the existing wf first** — `grep -rn 'rewrite_data_files\|expire_snapshots\|exp_iceberg\|hdfs_care' src/main/resources/wf/ctl/ src/main/resources/sql/dml/`. If found, reuse it (common existing names: `wf_schema_hdfs_care`, `wf_<table>_service`, project-specific variants). If not found, propose a new name following the project's convention — sensible options: `wf_schema_hdfs_care` (shared, one per schema), `wf_<table>_service` (per-table, easier scheduling), or `wf_iceberg_maintenance` (semantic-neutral). Every new Iceberg table needs `exp_iceberg_<table>.sql` and `upd_iceberg_<table>.sql` scripts under `src/main/resources/sql/dml/`.
  - Lock operations through CTL (`init_locks: checks/sets`, `*ctlCheckLockWrite`, `*ctlSetLockRead`).
  - Data-flow layers: `Sources → AUX (Parquet) → HIST (Iceberg) → AL (Iceberg)`. Read this section before deciding what to migrate vs. leave as parquet.

**If a recommendation in this `SKILL.md`, `examples.md`, `reference.md`, or generated `iceberg-runbook/` contradicts these guides, the guides win.** Specifically:

- Phase 2 in the phased runbook prescribes `CALL system.rewrite_data_files(...)` as a standalone call — in datamart projects that use the workflow system described in `ICEBERG_WF_GUIDE.md`, this MUST be wired through the project's maintenance wf (locate via grep first; see ICEBERG_WF_GUIDE "Сценарий 1" = reuse existing, "Сценарий 2" = create new per the project's naming convention).
- Step 3 "Ask the User for Iceberg Table Details" is **skipped** — the answer comes from `S2T_GUIDE.md` + the project's S2T Excel file.
- Step 6 "Create the Iceberg Table" — schema comes from S2T Excel, `TBLPROPERTIES` come from `ICEBERG_WF_GUIDE.md`, NOT from inference or interactive prompt.

### ⚠ MANDATORY — Iceberg Spark conf on every wf that touches a migrated table

For every workflow (`wf/ctl/*.yml`) that reads, writes, or runs any procedure (compaction, expire_snapshots, remove_orphan_files, MERGE, UPDATE, DELETE, ad-hoc spark-submit) against a migrated Iceberg table, the `spark_submit_cmd` (or the corresponding `--conf` block) **MUST include all three** of these conf flags. Adding two of three is a broken migration — the third one silently makes the catalog Hive-typed and queries fall back to the wrong code path.

```
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
--conf spark.sql.catalog.spark_catalog.type=hive
```

**Define the flags ONCE as a shared YAML parameter — do NOT inline them per wf.** Inlining means three flags get copy-pasted into every affected wf, drift over time, and the next dev has to remember to add them. The right pattern is:

1. **Search the project first** for an existing shared iceberg-conf parameter:

   ```bash
   grep -rn 'spark.sql.extensions.*IcebergSparkSessionExtensions\|SparkSessionCatalog' \
       src/main/resources/wf/ src/main/resources/devops/ src/main/resources/mart*.yml
   ```

   Typical existing names: `spark_submit_cmd_iceberg_service` (from `ICEBERG_WF_GUIDE.md`), `spark_iceberg`, `iceberg_conf`, project-specific. If found → reference it as `{{mart.<name>}}` in every affected wf's `spark_submit_cmd`. **Use the name that already exists, don't rename it.**
2. **If not found**, define a new shared parameter in `src/main/resources/mart.yml` (or the project's equivalent shared-config file). Propose 2–3 naming candidates to the user before adding — sensible options: `spark_iceberg` (short), `iceberg_conf` (semantic), `spark_submit_cmd_iceberg_service` (matches ICEBERG_WF_GUIDE convention). Example:

   ```yaml
   # src/main/resources/mart.yml (or shared config)
   spark_iceberg: >
     --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
     --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
     --conf spark.sql.catalog.spark_catalog.type=hive
   ```

   Then reference from every affected wf:

   ```yaml
   - param:
       name: spark_submit_cmd
       prior_value: "{{mart.spark_submit_cmd}} {{mart.spark_iceberg}}"
   ```

**Where to apply the reference (every place that touches a migrated table):**

- New per-table service wf (whatever name the maintenance wf has — see above) → `spark_submit_cmd` references `{{mart.<iceberg_conf_name>}}`.
- Existing wf that previously read/wrote the parquet table → patch its `spark_submit_cmd` to also reference `{{mart.<iceberg_conf_name>}}`. **Do not leave the old wf untouched** — same wf, new table format means new conf.
- Ad-hoc `spark-submit` in CI/CD or local runs → same three flags inline (no YAML there, so duplication is unavoidable).
- `iceberg-runbook/<ns>.<table>/phase1_add_files.sql` and `phase2_rewrite.sql` execution wrappers in such projects must also carry these flags.

The full Spark conf block from `ICEBERG_WF_GUIDE.md` "Spark-конфигурация для Iceberg" includes additional retry/partial-progress flags (`rewrite.partial-progress.*`, `commit.retry.*`). For compaction wf wrap the full block as `spark_submit_cmd_iceberg_service` (or whatever the project names the compaction-specific variant) and reference it. For other read/write wf the three flags above are the minimum.

Confirm in your announce that both guides were read AND that you will either reuse the project's existing iceberg-conf YAML parameter, or define a new one (named per the user's choice) and reference it from every affected wf — no inlining.

### ⚠ MANDATORY — analyze the pipeline, propose Iceberg-native alternatives (do NOT translate 1:1)

The skill rewrites individual call sites. But Iceberg unlocks pipeline-level simplifications that Parquet does not. **Before producing the worklist, scan the project for the patterns below and surface a concrete proposal to the user** rather than mechanically translating existing parquet-era SQL.

| Anti-pattern in the Parquet pipeline                                                                                                                                          | Iceberg-native replacement                                                                                                                                                                            | Why                                                                                                                                              |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| Custom "increment" SQL (`*_inc.sql`, `*_diff*.sql`, `*_changes*.sql`, `*_delta*.sql`) joining today's snapshot with yesterday's to detect inserts / updates / deletes | `MERGE INTO target USING source ON ...<br>``WHEN NOT MATCHED THEN INSERT *<br>``WHEN MATCHED AND (src.attr <> tgt.attr OR ...) THEN UPDATE SET *<br>``WHEN NOT MATCHED BY SOURCE THEN DELETE` | One statement does insert/update/delete atomically; no manual full-outer-join, no snapshot CTEs.                                                 |
| Reading "current" + "previous" snapshot and diffing them to emit change events downstream                                                                                     | `SELECT _change_type, * FROM target.changes` (Iceberg changelog scan)                                                                                                                               | `_change_type` is `INSERT`/`DELETE`/`UPDATE_BEFORE`/`UPDATE_AFTER` — Iceberg tracks this at the metadata layer, no diff query needed. |
| Manual tombstone columns (`is_deleted`, `deleted_at`) used because parquet has no row-level delete                                                                        | MoR +`format-version=2` + `write.delete.mode=merge-on-read` + ordinary `DELETE FROM`                                                                                                            | Iceberg deletes rows natively (position/equality deletes); the tombstone column becomes redundant.                                               |
| Full-partition rewrite for late-arriving data                                                                                                                                 | `MERGE INTO ... ON tgt.part_dt BETWEEN ... AND ...` (partition-aware MERGE)                                                                                                                         | Iceberg writes only affected files; old data files remain.                                                                                       |

**Detection signals:** SQL files named `*_inc.sql` / `*_diff*` / `*_changes*` / `*_delta*`, `FULL OUTER JOIN` / `LEFT JOIN ... WHERE x.id IS NULL` between two snapshots of the same logical table, `EXCEPT` / `MINUS` between filtered slices of the same source, or `WITH today AS (...), yesterday AS (...)` CTE pattern.

**Workflow:**

1. List each detected anti-pattern with file:line + the canonical replacement.
2. Ask the user to confirm before rewriting — they may have business reasons to keep the explicit increment logic (audit trail, downstream contract).
3. If the user accepts the MERGE-based rewrite, also propose retiring the now-redundant `*_inc.sql` and its `wf_*_inc` workflow entry — they would otherwise keep computing diffs against a table whose history is already in `.changes`.

Full pattern catalogue with before/after SQL examples: [reference.md § Iceberg-native pipeline optimizations](./reference.md#iceberg-native-pipeline-optimizations).

Confirm in your announce that you will run this pipeline analysis pass and surface findings to the user.

## What This Skill Does

Scans the project for Parquet and ORC operations (Hive DDL, Spark Dataset API, generic `format(...)` calls) and replaces them with Apache Iceberg equivalents:

- **Python:** pandas, PySpark (batch + generic format + streaming warn), pyarrow (classic + dataset warn) → pyiceberg
- **Java / Scala:** Spark Dataset API `spark.read().parquet|orc()` and `spark.read().format("parquet"|"orc").load()` → Iceberg Spark runtime (`format("iceberg")`)
- **Hive via SparkSQL:** `STORED AS PARQUET|ORC`, `USING parquet|orc`, `saveAsTable`, `INSERT INTO|OVERWRITE TABLE` → Iceberg-backed tables (`USING iceberg`, `writeTo(...)`)
- **Structured Streaming** and **pyarrow dataset/ParquetFile** are *detected* and left with `TODO(iceberg)` comments for manual rewrite.

Also updates project dependencies (`requirements.txt`, `pyproject.toml`, `pom.xml`, `build.gradle`).

For the full before/after rewrite matrix per language and the multi-table mapping format, see [examples.md](./examples.md).
For subsystem deep-dives (path schemes, constant folding, partition spec extraction, dynamic SQL loading, dry-run, phased runbook, operational concerns, known limitations) see [reference.md](./reference.md).

## Additional project-specific guides (optional)

`S2T_GUIDE.md` and `ICEBERG_WF_GUIDE.md` listed above are **mandatory** — they ship with this skill and always apply.

Beyond them, **also look for any `*_GUIDE.md` files at the project root of the project being migrated** — project owners use this naming convention for migration-relevant context. If such a guide contradicts SKILL.md defaults (and does not contradict the two mandatory skill guides), the project guide wins.

## Step-by-Step Process

### 1. Identify Project Type

Look for build files to determine stack:

- `requirements.txt` / `pyproject.toml` → **Python**
- `pom.xml` → **Java + Maven**
- `build.gradle` / `build.gradle.kts` → **Java/Scala + Gradle**
- `*.java` / `*.scala` files → JVM project

The skill targets all of these — the Bash recon commands in the Reconnaissance section cover `.py`, `.java`, and `.scala` files.

### 2. Detect Parquet / ORC / Hive Usage

Read source files and identify patterns. The skill scans for **both Parquet and ORC** and covers the Spark/pandas/pyarrow idioms below.

**Python:**

- pandas: `pd.read_parquet(...)` / `pd.read_orc(...)` / `.to_parquet(...)` / `.to_orc(...)`
- PySpark batch: `spark.read.parquet|orc(...)` / `df.write.parquet|orc(...)`
- PySpark generic: `spark.read.format("parquet"|"orc").load(...)` / `df.write.format(...).save(...)`
- PySpark streaming *(warn-only)*: `readStream.parquet|orc|format(...)`, `writeStream...`
- pyarrow classic: `pq.read_table(...)` / `pq.write_table(...)`
- pyarrow ORC: `orc.read_table(...)` / `orc.write_table(...)`
- pyarrow dataset *(warn-only)*: `pq.ParquetFile`, `pq.ParquetDataset`, `pa.dataset.dataset`, `pa.dataset.write_dataset`
- SparkSQL via `spark.sql(...)`: `STORED AS PARQUET|ORC`, `USING parquet|orc`, `INSERT INTO|OVERWRITE TABLE`

**Java:**

- Batch: `spark.read().parquet|orc(...)` / `df.write()...parquet|orc(...)`
- Generic: `spark.read().format("parquet"|"orc").load(...)` / `df.write()...format(...).save(...)`
- Streaming *(warn-only)*: `readStream()....`, `writeStream()....`
- `df.write().saveAsTable("...")`
- `spark.sql("CREATE [EXTERNAL] TABLE ... STORED AS PARQUET|ORC")`
- `spark.sql("CREATE TABLE ... USING parquet|orc")`
- `spark.sql("INSERT INTO|OVERWRITE TABLE ...")`

**Scala:** same as Java but with the parens-less `.read.parquet` / `.write.parquet` idiom.

For each detected pattern, refer to [examples.md](./examples.md) for the Iceberg equivalent.

### 3. Get table details from S2T (skip the interactive prompt)

**Per the mandatory [S2T_GUIDE.md](./S2T_GUIDE.md), table details come from S2T, NOT from asking the user.** Concrete steps:

1. Locate `s2t.xlsx` (or `hadoop_S2T_<PROJECT>_v<n>.xlsx`) under `<project>/src/main/resources/s2t/`.
2. Read sheets `Tables` (name, description, storage, location), `Columns` (name, type, nullability, description), `Partitions` (partition column + type).
3. The target Iceberg `(namespace, table)` = `(datamart_name from devops.json, table name from S2T)`. Storage column in S2T flips from `HIVE`/parquet to Iceberg.
4. Capture `entity_id` per table from `src/main/resources/wf/ctl/1_ctl_entities.yml`. You will need it in Step 6 and in the wf yaml.
5. Capture `ТУЗ` (yarn user, `u_<id>`) and `очередь ярн` (`root.g_<...>`) from `mart.yml` / `devops.json`. These go into the Gherkin scenario in `qaapi/*.feature`.

Catalog config comes from [ICEBERG_WF_GUIDE.md](./ICEBERG_WF_GUIDE.md) "Spark-конфигурация для Iceberg" — use `spark_catalog` with `type=hive`. Do NOT propose SQLite or REST — those are out of scope for these datamart projects.

Only ask the user when S2T is missing, ambiguous, or when a table is not yet declared in S2T (in which case add it to S2T first per S2T_GUIDE "Как добавить новую таблицу в S2T" before continuing).

### 4. Build the Worklist, Then Rewrite

There is no CLI detector in the markdown-only skill. Build the worklist manually from the I/O sites surfaced in the Reconnaissance step (section above): for each Parquet/ORC read or write call site, record the source file, line number, pattern type, and the target Iceberg `(namespace, table)` pair. Use `lakehouse-worklist.json` as the output filename to stay consistent with the reference docs.

For the mapping file format see the "Multi-table projects" section in [examples.md](./examples.md).

After the worklist is written, walk each task and apply the rewrite using the Conversion Reference tables in [examples.md](./examples.md). When all tasks are done, re-run the reconnaissance greps — the migrated patterns should be gone, and only `TODO(iceberg)` markers remain.

### 5. Review and Fix Edge Cases

After automated conversion, manually review:

- **Multiple tables** — the tool assumes one table per project; split and re-run per table if needed
- **Schema definitions** — Iceberg requires explicit schema. Extract from existing parquet:
  ```python
  import pyarrow.parquet as pq
  schema = pq.read_schema("existing.parquet")
  ```
- **Partitioning** — partition specifications are extracted structurally from `partitionBy(...)` / `bucketBy(...)` calls and propagated into the worklist. See "Partition spec extraction" in [reference.md](./reference.md) for what's supported and the code↔DDL mismatch detection.
- **Hive metastore catalog** — if the original project used Hive MetaStore, configure Iceberg's HiveCatalog:
  ```
  spark.sql.catalog.hive_prod = org.apache.iceberg.spark.SparkCatalog
  spark.sql.catalog.hive_prod.type = hive
  spark.sql.catalog.hive_prod.uri = thrift://metastore:9083
  ```
- **Existing Hive tables with data** — use Iceberg's `system.migrate` procedure to convert in place:
  ```sql
  CALL hive_prod.system.migrate('db.events')
  ```

For other known caveats (FQN propagation, streaming, pyarrow dataset, viewfs, etc.) see "Known Limitations" in [reference.md](./reference.md).

### 6. Create the Iceberg Table

**Schema is generated from S2T Excel (per S2T_GUIDE.md), TBLPROPERTIES come from ICEBERG_WF_GUIDE.md, catalog is `spark_catalog` with `type=hive`. Do not invent schemas or properties.**

Concrete:

1. Generate / regenerate DDL from `s2t.xlsx` per S2T_GUIDE "Контрольный список перед запуском" (the DDL generator step that turns S2T into `src/main/resources/sql/ddl/<layer>/<table>.sql`). Then change `STORED AS PARQUET` → `USING iceberg`.

**MoR (merge-on-read)** — для таблиц с UPDATE / DELETE / MERGE:

```sql
CREATE TABLE {{datamart_name}}.<table> (
  -- columns FROM S2T sheet `Columns` — types/nullability AS-IS from S2T
)
USING iceberg
PARTITIONED BY (
  -- FROM S2T sheet `Partitions` — typically (ctl_loading INT) or part_report_dt.
  -- Partition column type is preserved AS-IS from the parquet/S2T side (e.g. if the
  -- existing parquet partitions by `ctl_validfrom BIGINT`, keep BIGINT — do NOT propose
  -- a transform like days(...) or a TIMESTAMP cast). Upstream pipelines already produce
  -- values of the original type, and Phase 1 `add_files` requires partition-type parity
  -- with the existing parquet. If the user explicitly wants a transform, ask first.
)
TBLPROPERTIES (
  'format-version'                   = '2',
  'write.parquet.compression-codec'  = 'zstd',
  'write.update.mode'                = 'merge-on-read',
  'write.delete.mode'                = 'merge-on-read',
  'write.merge.mode'                 = 'merge-on-read'
);
```

**CoW (copy-on-write)** — для таблиц без row-level мутаций (только INSERT / overwrite):

```sql
CREATE TABLE {{datamart_name}}.<table> (
  -- columns FROM S2T sheet `Columns` — types/nullability AS-IS from S2T
)
USING iceberg
PARTITIONED BY (
  -- FROM S2T sheet `Partitions` — typically (ctl_loading INT) or part_report_dt.
  -- Partition column type is preserved AS-IS from the parquet/S2T side (e.g. if the
  -- existing parquet partitions by `ctl_validfrom BIGINT`, keep BIGINT — do NOT propose
  -- a transform like days(...) or a TIMESTAMP cast). Upstream pipelines already produce
  -- values of the original type, and Phase 1 `add_files` requires partition-type parity
  -- with the existing parquet. If the user explicitly wants a transform, ask first.
)
TBLPROPERTIES (
  'format-version'                   = '2',
  'write.parquet.compression-codec'  = 'zstd',
  'write.update.mode'                = 'copy-on-write',
  'write.delete.mode'                = 'copy-on-write',
  'write.merge.mode'                 = 'copy-on-write'
);
```

Mode selection rule: if the user has explicitly named a mode — use it (see "Mode selection — user input is ground truth" earlier in this file). Otherwise: MoR if DML scripts contain `UPDATE`/`DELETE FROM`/`MERGE INTO`, CoW otherwise. Always write all three `write.*.mode` properties explicitly — never rely on the Iceberg default, as it differs between format versions and catalog implementations.

2. Wire the table into compaction per [ICEBERG_WF_GUIDE.md](./ICEBERG_WF_GUIDE.md) — this is **not optional**:

   - Create `src/main/resources/sql/dml/exp_iceberg_<table>.sql` (expire_snapshots) — template in ICEBERG_WF_GUIDE "Expire snapshots only".
   - Create `src/main/resources/sql/dml/upd_iceberg_<table>.sql` (full cycle) — **template choice depends on mode**:
     - **MoR table** (`write.delete.mode=merge-on-read`): use the MoR-aware template — "Полный цикл обслуживания для MoR-таблиц" in ICEBERG_WF_GUIDE. It adds `delete-file-threshold` to `rewrite_data_files`, plus `rewrite_position_delete_files` and `rewrite_manifests` calls. **Required for MoR** — without it, position-delete files accumulate, reads degrade silently.
     - **CoW table** (default): use "Полный цикл обслуживания" — basic `rewrite_data_files` + `remove_orphan_files`.
   - Using the CoW template on a MoR table is a silent footgun. The Phase B plan table records the chosen mode per table; cross-check it here.
   - Add the two `spark_driver_extraJavaOptions__hdfs_care_*` params for the table to the **project's existing maintenance wf** in `src/main/resources/wf/ctl/`. Find it first: `grep -rn 'rewrite_data_files\|expire_snapshots\|exp_iceberg\|hdfs_care' src/main/resources/wf/ctl/`. Common names that may already exist: `wf_schema_hdfs_care`, `wf_<table>_service`, or a project-specific variant. If nothing found, follow ICEBERG_WF_GUIDE "Сценарий 2: Создай отдельный wf для таблицы" and pick a name matching the project's convention — propose 2–3 candidates to the user (`wf_<table>_service` for per-table, `wf_schema_hdfs_care` / `wf_iceberg_maintenance` for shared) and confirm before creating.
   - Use `entity_id` captured in Step 3.
3. Update the Gherkin scenario (`ift.feature` / `st_skl.feature`) per S2T_GUIDE "Шаг 4: Добавь сценарий в Gherkin-файл" — add the new table to the DDL check sub-scenarios.

The `iceberg-runbook/<ns>.<table>/phase2_rewrite.sql` emitted by the migrator is a **template** — in such projects, replace standalone execution with the project's maintenance wf wiring described above. Phase 1 (`add_files`) and Phase 3 (switchover) still apply.

For the rest of the phased rollout (Phase 1 `add_files`, Phase 3 switchover options) and operational background (MoR/CoW, snapshot expiration), see [reference.md](./reference.md) sections "Phased migration runbook" and "Post-Migration Operational Concerns" — but read them through the lens of ICEBERG_WF_GUIDE.md (wf/ctl wiring, not standalone Spark jobs).

### 7. Run Existing Tests

**Python** (install pyiceberg in the project's own venv or per the project's dependency instructions, then):

```bash
pytest tests/ -v
```

**Java/Maven:**

```bash
mvn test
```

**Scala/Gradle:**

```bash
./gradlew test
```

Common test failures:

- Tests use `tmp_path` for parquet file path but Iceberg catalog uses a fixed URI — inject catalog via fixture
- Assertions on file existence (`Path("data.parquet").exists()`) — replace with table existence checks

### 8. Commit

```bash
git add -A
git commit -m "refactor: migrate parquet read/write to Apache Iceberg"
```

## Where to look next

- **[examples.md](./examples.md)** — conversion reference tables (Python, Java/Scala, Hive/SparkSQL), dependencies added per ecosystem, multi-table mapping file format.
- **[reference.md](./reference.md)** — operational concerns (MoR/CoW, compaction, snapshot expiration), path schemes (s3/hdfs/abfs/gs/viewfs/file), constant folding rules, partition spec extraction, dynamic SQL loading, dry-run mode, phased migration runbook, known limitations.
