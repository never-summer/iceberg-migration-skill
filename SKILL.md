---
name: iceberg-migration-skill
description: Convert Parquet/ORC read/write to Apache Iceberg in Python, Java, or Scala projects. Use when the user says "convert parquet", "migrate to iceberg", "parquet to iceberg", "migrate hive to iceberg", "convert orc", "migrate orc to iceberg", or asks to move Hive-parquet tables to Iceberg.
---
# Parquet/ORC → Iceberg Conversion Skill

This skill is a **state machine with 6 steps and 3 STOP gates**. You are always in exactly one step. Never skip a step; never combine two STOP-gated steps into one message.

```
STEP 0 memory → STEP 1 recon → STEP 2 INTERVIEW ⛔STOP → STEP 3 PLAN ⛔STOP("go") → STEP 4 apply → STEP 5 VERIFY LOOP ⛔STOP(report)
```

## HARD RULES — a run violating any of these is a failed run

- **R1 — Interview before mode.** No table gets a MoR/CoW decision and no DML gets rewritten before its row in the STEP 2 interview table is confirmed by the user.
- **R2 — MoR needs pre-existing mutations.** MoR only for tables that ALREADY receive row-level `UPDATE`/`DELETE`/`MERGE` in the pre-migration pipeline. Append, SCD-versioned (`ctl_validfrom`/`ctl_validto`, partition on load/valid timestamp), `INSERT OVERWRITE`, and log tables are NOT MoR. A user's blanket "migrate with MoR" sets the mode only for qualifying tables — the rest are flagged in the interview, never silently flipped. A `MERGE` that this skill itself proposes is never evidence for MoR.
- **R3 — AUX/INI stay Parquet by default.** Per ICEBERG_WF_GUIDE the flow is `Sources → AUX (Parquet) → HIST (Iceberg) → AL (Iceberg)`. AUX/INI/staging tables enter the plan as `keep parquet` rows; migrating one requires an explicit user "yes" recorded in decisions.yml.
- **R4 — Never invent values.** Every entity_id, Spark conf key, TBLPROPERTIES value, wf name, YAML anchor must be COPIED from this project, from a sibling project (then ADAPTED: prefixes/schemas/paths of THIS project), or from the guides. A value you cannot point to a source for = ask the user. This includes Spark conf keys: the iceberg conf param contains EXACTLY the 3 flags from the guide plus only sibling-verified extras — do not compose plausible-looking `spark.sql.catalog.*` keys from memory (procedure options like `partial-progress` and table properties like `commit.retry.*` are NOT Spark confs).
- **R5 — No table migrates without maintenance.** Every table whose final mode is Iceberg (MoR or CoW) gets `exp_iceberg_<table>.sql` + `upd_iceberg_<table>.sql` AND an entry in the maintenance wf — in the same run. If you migrate 8 tables, maintenance covers 8, not 5.
- **R6 — Every reader gets the conf.** Every wf in ctl.yml that reads OR writes any migrated table gets `{{mart.<iceberg_conf_param>}}`; wf running MERGE additionally gets the merge conf param. Find readers by grepping ctl.yml/DML for the table name — not from memory.
- **R7 — Done means verified.** "Done" may only be declared by STEP 5 after a probe pass with zero failures. One grep is not verification.

## STEP 0 — memory (first action after the announce)

Announce verbatim:
> "I'm using the iceberg-migration-skill (v3 state machine)."
> "Order: recon → per-table interview (STOP) → plan (STOP, wait for go) → apply → verify loop. Decisions persist in ./.iceberg-migration/decisions.yml; on a re-run I only re-ask what changed."

Then: `cat ./.iceberg-migration/decisions.yml` — if it exists, reuse every recorded answer (say which) and only ask deltas in STEP 2. If not, create it now with the header below and fill it as answers arrive (write immediately after each answer, not in a batch):

```yaml
# maintained by iceberg-migration-skill — commit together with the migration
version: 3
project: <datamart>
global:
  iceberg_conf_param: <name>            # copied/adapted, source noted
  maintenance_wf: <name>                # adapted to THIS project's wf_prefix — no sibling literals
  compression_codec: {value: <v>, source: "sibling <proj> ddl"}
tables:
  <schema>.<table>:
    layer: AUX|INI|HIST|AL              # AUX/INI default keep-parquet (R3)
    write_pattern: append|overwrite|upsert|scd_versioned
    unique_key: [..]                    # of a PHYSICAL row; scd_versioned MUST include the version column
    preexisting_mutations: none|<file:line>
    mode: keep-parquet|CoW|MoR
    mode_basis: "user-confirmed interview row"|"pre-existing MERGE <file:line>"   # never a MERGE this skill added
    merge_rewrite_approved: true|false|n/a  # interview Q3 answer for this table
    entity_id: <id>                     # copied from ctl.yml/1_ctl_entities.yml — never invented (R4)
    status: planned|applied|verified
verify_log: []                          # appended by STEP 5, one entry per pass
```

## STEP 1 — recon (read-only)

1. Read all four guides: S2T_GUIDE.md, ICEBERG_WF_GUIDE.md, reference.md, examples.md.
2. Run this block (substitute names) and keep the outputs — STEP 2 pre-fill, STEP 4.2 and probes P1/P5 consume them:

```bash
grep -rn "STORED AS PARQUET\|USING parquet\|USING iceberg" src/main/resources/sql/ddl/          # scope + already-Iceberg
grep -rln "UPDATE \|DELETE FROM\|MERGE INTO" src/main/resources/sql/dml/                        # pre-existing mutations (R2)
grep -rn "ctl_validfrom\|ctl_validto\|part_ctl_" src/main/resources/sql/ddl/<domain>/           # SCD tells → write_pattern
ls src/main/resources/sql/dml/ | grep -i "_inc\|_diff\|_delta\|_changes"                        # anti-patterns
for t in <tables>; do echo "== $t"; grep -n "$t" src/main/resources/wf/ctl/ctl.yml; done        # full reader/writer wf list
grep -rn "iceberg\|hdfs_care" src/main/resources/wf/ctl/mart.yml <sibling>/wf/ctl/mart.yml      # existing infra + sibling reference
```

3. From the outputs, pre-fill the STEP 2 table. Sibling values are templates to ADAPT (R4), never to paste.

## STEP 2 — INTERVIEW ⛔ STOP — this message contains ONLY the block below and ends your turn

Fill every cell yourself from recon (infer first — a wrong guess costs the user one correction). Rows already answered in decisions.yml: mark `— reused` and don't re-ask.

```
### Per-table interview — confirm or correct each row; the plan is blocked on this
| # | Table | Layer | Write pattern | Unique key (physical row) | Pre-existing UPD/DEL/MERGE? | Proposed | Basis |
|---|-------|-------|---------------|---------------------------|------------------------------|----------|-------|
| 1 | aux.t_x | AUX | scd_versioned (ctl_validfrom/to, part_ctl_validfrom) | (bk…, ctl_validfrom) | none | keep parquet (R3) | layer rule |
| 2 | pa.t_y  | AL  | upsert | (epk_id) — from MERGE ON, unverified | yes — t_y.sql:442 | Iceberg MoR | pre-existing MERGE |

Questions (answer by row number):
1. Layers/modes: confirm rows as proposed? AUX/INI rows stay Parquet unless you explicitly pull one in (name it).
2. Any row where my write-pattern or key guess is wrong?
3. Rows where I propose rewriting SELECT/INSERT → MERGE INTO: <list or "none"> — Y/N each.
4. Non-default values I could not source from the sibling: <codec/file-size/sort/bloom or "none — all sourced">.
```

Write every answer to decisions.yml, then go to STEP 3.

## STEP 3 — PLAN ⛔ STOP — wait for "go"

The plan message is INVALID unless it physically contains, in this order: (a) the confirmed interview table from STEP 2, (b) the pasted current content of decisions.yml, (c) the sections below. If (a) or (b) is missing you are still in STEP 2.

```
### Migration plan — <project>/<domain>
[confirmed interview table here]
[--- decisions.yml (current) ---
<paste file>
---]

Files to change:
- mart.yml — add <iceberg_conf_param> (exactly the 3 guide flags + sibling-verified extras, each with source), <merge conf if any MERGE flow>, ssc/wf vars for maintenance
- ctl.yml — patch spark_submit_cmd of THESE wf (full reader/writer list from recon step 3): <list — must cover every wf touching every migrated table, R6>
- ctl.yml — add maintenance wf <name> covering THESE tables: <every non-keep-parquet row, R5>
- sql/ddl/… — per non-keep-parquet row: USING iceberg + FULL TBLPROPERTIES per reference table (all 11 defaults; fewer only if a row-specific decision says so)
- sql/dml/exp_iceberg_<t>.sql + upd_iceberg_<t>.sql — per non-keep-parquet row; where-predicate = THAT table's own PARTITIONED BY column; unpartitioned → binpack, no where
- (only if approved in interview Q3) <table>_inc.sql → MERGE/.changes rewrite

Out of scope (and why): <keep-parquet rows; untouched wf>
Reply "go" to apply, or list changes.
```

## STEP 4 — apply (order matters)

1. **mart.yml** — conf params. The iceberg param = the 3 mandatory flags (extensions / SparkSessionCatalog / catalog.type=hive — exact strings in ICEBERG_WF_GUIDE) + only extras you can cite from the sibling (R4). Shared param, referenced as `{{mart.<param>}}` — never inline the flags per-wf.
2. **ctl.yml, existing wf** — add `{{mart.<iceberg_conf_param>}}` to every wf on the recon reader/writer list; add the merge param to wf executing MERGE.
3. **DDL** — per table: `USING iceberg`, `PARTITIONED BY` with column type AS-IS from the parquet side, and this full block — sourcing rules for each `<value>` are in **ICEBERG_WF_GUIDE.md § "DDL: full TBLPROPERTIES template"** (codec from sibling; file-size agreed with the compaction script; bloom only when justified per table, else omit; comment verbatim from S2T):

```sql
TBLPROPERTIES (
  'format-version'                             = '2',
  'write.format.default'                       = 'parquet',
  'write.parquet.compression-codec'            = '<from sibling>',
  'write.target-file-size-bytes'               = '<match upd_ script>',
  'write.distribution-mode'                    = 'none',            -- 'hash' only with user-confirmed reason
  'write.update.mode'                          = '<merge-on-read|copy-on-write>',
  'write.delete.mode'                          = '<same as above>',
  'write.merge.mode'                           = '<same as above>',
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max'       = '10',              -- keep equal to app.sql.retain_snapshots
  'comment'                                    = '<S2T Tables sheet, verbatim>'
);
```

Already-Iceberg tables: diff their existing block against this one and backfill missing properties in the same edit. Sanity: fewer than 11 keys without a recorded row decision → go back.
4. **Compaction scripts** — copy the guide template per table; substitute `<partition_column>` with THAT table's `PARTITIONED BY` column (aux `part_ctl_validfrom` ≠ pa `part_ctl_datechange`); unpartitioned → strategy binpack, no `where`. MoR tables get the MoR-aware template (delete-file-threshold + rewrite_position_delete_files + rewrite_manifests).
5. **ctl.yml, maintenance wf** — name adapted to this project's prefix; entries for EVERY migrated table; params `app.sql.service_date_from` / `safe_days` / `retain_snapshots` defined and format-compatible with the partition values (a `yyyyMMdd` bound against `yyyyMMddHHmmss` BIGINT partitions compares wrong — match digits).
6. **Gherkin/S2T** if the project has them.
7. Update each table's `status: applied` in decisions.yml.

⚠ While editing DML, do NOT convert an append/SCD/overwrite load into `MERGE INTO` on the business key: on a versioned target it either silently rewrites all historical versions (`WHEN MATCHED UPDATE SET *`) or dies on a cardinality violation. MERGE only where interview said `upsert` + Q3 approved; `ON` key = the confirmed unique key.

## STEP 5 — VERIFY → REPAIR ⛔ loop, max 3 passes

Run this block literally (substitute names), collect failures, append `{pass, failed, fixed}` to `verify_log`, fix, re-run the WHOLE block (a fix can break a previously green probe). Zero failures → report done. Failures after pass 3 → report them as OPEN BLOCKERS, not done.

```bash
# P1 residual parquet — whole project, then compare hits against EVERY plan row
grep -rn "STORED AS PARQUET\|USING parquet" src/main/resources/ 
# hits allowed ONLY on keep-parquet rows; keep-parquet rows MUST still hit (untouched)

# P2 maintenance coverage — per migrated table, all three must return >0
for t in <migrated tables>; do
  ls src/main/resources/sql/dml/*iceberg*$t* ; grep -c "$t" src/main/resources/wf/ctl/ctl.yml ; done
# MoR tables additionally: grep -l "rewrite_position_delete_files\|delete-file-threshold" their upd_ file

# P3 params resolve — every ${app.sql.*} used by scripts is defined in the wf/mart.yml
grep -roh 'app\.sql\.[a-z_]*' src/main/resources/sql/dml/*iceberg*.sql | sort -u   # each must appear in ctl.yml/mart.yml

# P4 predicate vs partition — for each upd_ file: where-column ∈ that table's PARTITIONED BY (read the DDL); unpartitioned → no where. Also digit-length of the bound matches the partition format.
grep -n "where =>" src/main/resources/sql/dml/upd_iceberg_*.sql

# P5 readers covered — every wf that mentions a migrated table has the conf param
for t in <migrated tables>; do grep -n "$t" src/main/resources/wf/ctl/ctl.yml ; done   # each wf found → check it references {{mart.<iceberg_conf_param>}}

# P6 conf keys are sourced — every --conf key in the new mart.yml params exists verbatim in the sibling or the guide
# P7 no sibling literals + wf integrity — grep -rn "<sibling_prefix>" src/main/resources/wf/ctl/ → must be empty;
#    the new wf's YAML anchors (*wfDefaultInfo etc.) and its oozie app path (hdfs_care) exist in THIS project
# P8 entity_id provenance — every entity.id you added appears elsewhere: grep -rn "<id>" src/main/resources/ beyond your edit
# P9 TBLPROPERTIES completeness — per new/edited DDL:
grep -c "write\.\|format-version\|comment" src/main/resources/sql/ddl/<domain>/*.sql   # ≥ 10 per file (11 props; bloom only when justified) unless a recorded row decision says fewer
# P10 MoR justification — every mode:MoR row in decisions.yml has mode_basis = pre-existing <file:line> or user-confirmed; its unique_key matches any MERGE ON writing to it
```

Final report = summary + last verify pass + decisions.yml path. Nothing else counts as done.

## Reference
Details live in the guides — TBLPROPERTIES reference table and DDL/DML/wf templates: ICEBERG_WF_GUIDE.md + reference.md; S2T: S2T_GUIDE.md; worked examples: examples.md. When a template and this file disagree, the guides win for file contents; this file wins for process.
