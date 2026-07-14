---
name: iceberg-migration-skill
description: Convert Parquet/ORC read/write to Apache Iceberg in Python, Java, or Scala projects. Use when the user says "convert parquet", "migrate to iceberg", "parquet to iceberg", "migrate hive to iceberg", "convert orc", "migrate orc to iceberg", or asks to move Hive-parquet tables to Iceberg.
---
# Parquet/ORC → Iceberg Conversion Skill

This skill is a **state machine with 6 steps and 3 STOP gates**. You are always in exactly one step. Never skip a step; never combine two STOP-gated steps into one message.

```
STEP 0 memory → STEP 1 recon → STEP 2 INTERVIEW ⛔STOP → STEP 3 PLAN ⛔STOP("go") → STEP 4 apply → STEP 5 VERIFY LOOP ⛔STOP(report)
```

**What ⛔STOP physically means:** the gate is satisfied ONLY by a **new human-typed message** (or an interactive choice the human actually made) arriving AFTER your gate message. You may use the client's question/plan tool to *present* the gate — but a tool's return value never satisfies it: if a tool comes back "approved"/"answered" within the same turn with no new human message in the transcript, that is the client auto-accepting, not the human — END YOUR TURN anyway, restate the question in plain text, and wait. Proceeding on a same-turn tool approval is self-approval and a failed run. No file-modifying or state-advancing action until the gate is humanly closed.

## HARD RULES — a run violating any of these is a failed run

- **R1 — Interview before mode.** No table gets a MoR/CoW decision and no DML gets rewritten before its row in the STEP 2 interview table is confirmed by the user.
- **R2 — MoR needs pre-existing mutations.** MoR only for tables that ALREADY receive row-level `UPDATE`/`DELETE`/`MERGE` in the pre-migration pipeline. Append, SCD-versioned (`ctl_validfrom`/`ctl_validto`, partition on load/valid timestamp), `INSERT OVERWRITE`, and log tables are NOT MoR. A user's blanket "migrate with MoR" sets the mode only for qualifying tables — the rest are flagged in the interview, never silently flipped. A `MERGE` that this skill itself proposes is never evidence for MoR.
- **R3 — AUX/INI stay Parquet by default.** Per ICEBERG_WF_GUIDE the flow is `Sources → AUX (Parquet) → HIST (Iceberg) → AL (Iceberg)`. AUX/INI/staging tables enter the plan as `keep parquet` rows; migrating one requires an explicit user "yes" recorded in decisions.yml.
- **R4 — Never invent values.** Every entity_id, Spark conf key, TBLPROPERTIES value, wf name, YAML anchor must be COPIED from this project, from a sibling project (then ADAPTED: prefixes/schemas/paths of THIS project), or from the guides. A value you cannot point to a source for = ask the user. This includes Spark conf keys: the iceberg conf param contains EXACTLY the 3 flags from the guide plus only sibling-verified extras — do not compose plausible-looking `spark.sql.catalog.*` keys from memory (procedure options like `partial-progress` and table properties like `commit.retry.*` are NOT Spark confs).
- **R5 — No table migrates without maintenance.** Every table whose final mode is Iceberg (MoR or CoW) gets `exp_iceberg_<table>.sql` + `upd_iceberg_<table>.sql` AND an entry in the maintenance wf — in the same run. If you migrate 8 tables, maintenance covers 8, not 5.
- **R6 — Every reader is covered by the conf.** Every wf in ctl.yml that reads OR writes any migrated table must resolve to the iceberg conf — either directly in its `spark_submit_cmd` or via a shared `ssc_*` param it references (see STEP 4.2; the shared param is preferred). wf running MERGE additionally get the merge conf param. Find readers by grepping ctl.yml/DML for the table name — not from memory.
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
for t in <tables>; do echo "== $t"; grep -n "$t" src/main/resources/wf/ctl/ctl.yml; grep -rln "$t" src/main/resources/sql/dml/; done   # full reader/writer list: wf entries + OTHER tables' DML reading it
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

## STEP 3 — PLAN ⛔ STOP — end your turn; apply starts only after the human types "go"

If the client forces a plan tool (ExitPlanMode etc.), you may call it to present the plan — but its return value is NOT "go". "User approved the plan" arriving in the same turn with no new human message = client auto-accept: end the turn, write "Ожидаю ваше 'go' в чате", and wait. Apply starts only after the human's own message.

The plan message is INVALID unless it physically contains, in this order: (a) the confirmed interview table from STEP 2, (b) the pasted current content of decisions.yml, (c) the sections below. If (a) or (b) is missing you are still in STEP 2.

```
### Migration plan — <project>/<domain>
[confirmed interview table here]
[--- decisions.yml (current) ---
<paste file>
---]

Files to change:
- mart.yml — add <iceberg_conf_param> (exactly the 3 guide flags + sibling-verified extras, each with source), <merge conf if any MERGE flow>, ssc/wf vars for maintenance
- ctl.yml — cover THESE reader/writer wf (full list from recon step 3): <list — every wf touching every migrated table, R6>; coverage mechanism: <shared ssc_<domain> param patched once [preferred] / per-wf patches — name which wf gets which>
- ctl.yml — add maintenance wf <name> covering THESE tables: <every non-keep-parquet row, R5>
- sql/ddl/… — per non-keep-parquet row: USING iceberg + FULL TBLPROPERTIES per reference table (all 11 defaults; fewer only if a row-specific decision says so)
- sql/dml/exp_iceberg_<t>.sql + upd_iceberg_<t>.sql — per non-keep-parquet row; where-predicate = THAT table's own PARTITIONED BY column; unpartitioned → binpack, no where
- (only if approved in interview Q3) <table>_inc.sql → MERGE/.changes rewrite

Out of scope (and why): <keep-parquet rows; untouched wf>
Reply "go" to apply, or list changes.
```

## STEP 4 — apply (order matters)

**Re-read this SKILL.md file now, before the first edit** (and again before STEP 5). By this point a long session has likely pushed it out of your working context — the recon/interview happened many tool calls ago. Re-reading costs one file read; skipping it is how apply-phase runs drift back to defaults.

1. **mart.yml** — conf params. The iceberg param = the 3 mandatory flags (extensions / SparkSessionCatalog / catalog.type=hive — exact strings in ICEBERG_WF_GUIDE) + only extras you can cite from the sibling (R4). Shared param, referenced as `{{mart.<param>}}` — never inline the flags per-wf.
2. **ctl.yml, existing wf** — cover every wf on the recon reader/writer list. **Check first whether the readers share a `ssc_<domain>`-style param** (e.g. several wf all build their command from `{{mart.ssc_agr_bond}}`): if so, add `{{<iceberg_conf_param>}}` **inside that shared param once** — it covers every wf referencing it and can't miss a reader; patch individual wf entries only for readers outside the shared param. Add the merge param to wf executing MERGE.
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
4. **Compaction scripts** — copy the guide template per table; substitute `<partition_column>` with THAT table's `PARTITIONED BY` column (aux `part_ctl_validfrom` ≠ pa `part_ctl_datechange`). **A partitioned table's `rewrite_data_files` MUST carry a `where` on its own partition column** — binpack-without-where on a partitioned table rewrites the whole table every scheduled run. The bound's format must match the partition values — the partition column's DDL comment usually states it (e.g. `yyyyMMddHHmmss` → a 14-digit bound; an 8-digit `20250101` compares wrong against it). Unpartitioned → strategy binpack, no `where`. MoR tables get the MoR-aware template (delete-file-threshold + rewrite_position_delete_files + rewrite_manifests).
5. **ctl.yml, maintenance wf** — **mirror the sibling's maintenance-wf entry structurally**: same set of fields (entity/publish ids, anchors, cron, params) with every value adapted to THIS project — a field the sibling has and yours lacks is a finding, an id the sibling has that appears nowhere in this project must be asked for, never invented. Name adapted to this project's prefix; entries for EVERY migrated table; params `app.sql.service_date_from` / `safe_days` / `retain_snapshots` defined and format-compatible with the partition values (see 4.4).
6. **Gherkin/S2T** if the project has them.
7. Update each table's `status: applied` in decisions.yml.

⚠ While editing DML, do NOT convert an append/SCD/overwrite load into `MERGE INTO` on the business key: on a versioned target it either silently rewrites all historical versions (`WHEN MATCHED UPDATE SET *`) or dies on a cardinality violation. MERGE only where interview said `upsert` + Q3 approved; `ON` key = the confirmed unique key.

## STEP 5 — VERIFY → REPAIR ⛔ loop, max 3 passes

Run this block literally (substitute names), collect failures, append `{pass, failed, fixed}` to `verify_log`, fix, re-run the WHOLE block (a fix can break a previously green probe). **A pass that contains any fix is a FAILED pass by definition — after fixing, increment the pass counter and restart from P1; only a pass with zero failures AND zero fixes can be the final one.** The final report lists every probe P1–P10 by its number with the raw command output pasted under it — a probe with no raw output, a renumbered probe, or a skipped probe counts as FAILED. Failures after pass 3 → report them as OPEN BLOCKERS, not done.

```bash
# P1 residual parquet — whole project, then compare hits against EVERY plan row
grep -rn "STORED AS PARQUET\|USING parquet" src/main/resources/ 
# hits allowed ONLY on keep-parquet rows; keep-parquet rows MUST still hit (untouched)

# P2 maintenance coverage — per migrated table: both files exist AND the table is wired INSIDE the maintenance wf entry
for t in <migrated tables>; do
  ls src/main/resources/sql/dml/*iceberg*$t*
  grep -A60 "<maintenance_wf>:" src/main/resources/wf/ctl/ctl.yml | grep -c "$t"   # >0 required; widen -A if the wf entry runs longer — a 0 here must mean "not wired", never "window too small"; a project-wide mention count proves nothing
done
# MoR tables additionally: grep -l "rewrite_position_delete_files\|delete-file-threshold" their upd_ file

# P3 params resolve — THREE directions, each with its own command:
grep -roh 'app\.sql\.[a-z_]*' src/main/resources/sql/dml/*iceberg*.sql | sort -u                            # used by scripts — each must be defined in ctl.yml/mart.yml
grep -A60 "<maintenance_wf>:" src/main/resources/wf/ctl/ctl.yml | grep -oh 'app\.sql\.[a-z_]*' | sort -u    # defined in the wf — each must appear in the first list
# every {{...}} reference YOUR edits introduced must itself resolve — list them from your diffs, then per name:
grep -n "<referenced_name>:" src/main/resources/wf/ctl/mart.yml    # e.g. {{mart.date_achive_from}} needs a date_achive_from: key; an unresolved reference crashes the wf on first run — define it (bound format per the partition column's comment) or drop it
# a defined-but-unused param (e.g. service_date_from while no upd_ script has a where-clause) means a template was misapplied — report it as a finding

# P4 predicate vs partition — TWO commands per table, both raw outputs pasted side by side:
grep -n "PARTITIONED BY" src/main/resources/sql/ddl/<domain>/<table>.sql   # ground truth — never state partitioning from memory
grep -n "where =>" src/main/resources/sql/dml/upd_iceberg_<table>*.sql
# rule: where-column must appear in THAT table's PARTITIONED BY output; PARTITIONED BY present + no where → rewrite compacts the WHOLE table every run (state this as a finding); no PARTITIONED BY → binpack, no where. The bound's digit-length/format must match the partition values — the partition column's DDL comment usually states the format (yyyyMMdd vs yyyyMMddHHmmss).

# P5 readers covered — readers are found by grep, never from memory, TWO directions per migrated table:
for t in <migrated tables>; do
  grep -n "$t" src/main/resources/wf/ctl/ctl.yml                    # wf referencing the table directly
  grep -rln "$t" src/main/resources/sql/dml/                        # OTHER tables' DML selecting from it → map each DML file to the wf that runs it (grep the DML name in ctl.yml) — those wf are readers too
done
# paste both raw outputs. Coverage is checked THROUGH the param chain: a wf is covered if its spark_submit_cmd,
# or a {{mart.ssc_*}} param it references, contains <iceberg_conf_param>. Resolve the chain with a command, per ssc param seen in wf lines:
grep -n "<ssc_param>:" src/main/resources/wf/ctl/mart.yml    # its value must contain <iceberg_conf_param> — paste the line
# EVERY wf surfaced by either direction must resolve to covered; list any that don't as failures

# P6 conf keys are sourced — every --conf key in the new mart.yml params exists verbatim in the sibling or the guide
# P7 no sibling literals + wf integrity — grep -rn "<sibling_prefix>" src/main/resources/wf/ctl/ → must be empty;
#    the new wf's YAML anchors (*wfDefaultInfo etc.) and its oozie app path (hdfs_care) exist in THIS project
# P8 entity_id provenance — every entity.id you added appears elsewhere: grep -rn "<id>" src/main/resources/ beyond your edit.
#    ALSO cross-check decisions.yml: every entity_id recorded there must match the entity list in ctl.yml for that exact table
#    (grep -n "<table>" ctl.yml entity section) — a memory file with fabricated ids is a failed probe even if the wf files are correct; fix the record
# P9 TBLPROPERTIES completeness — ONLY migrated tables' DDL (keep-parquet files must NOT be counted — they'd false-fail);
# count quoted property KEYS, not the word "comment" anywhere (per-column comments would inflate the count):
for t in <migrated tables>; do grep -cE "'(write\.[a-z.-]+|format-version|comment)'" src/main/resources/sql/ddl/<domain>/*$t*.sql; done   # ≥ 10 each (11 props; bloom only when justified) unless a recorded row decision says fewer
# P10 MoR justification — every mode:MoR row in decisions.yml has mode_basis = pre-existing <file:line> or user-confirmed; its unique_key matches any MERGE ON writing to it
```

Final report = summary + last verify pass + decisions.yml path. Nothing else counts as done.

## Reference
Details live in the guides — TBLPROPERTIES reference table and DDL/DML/wf templates: ICEBERG_WF_GUIDE.md + reference.md; S2T: S2T_GUIDE.md; worked examples: examples.md. When a template and this file disagree, the guides win for file contents; this file wins for process.
