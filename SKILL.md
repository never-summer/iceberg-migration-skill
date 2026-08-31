---
name: iceberg-migration-skill
description: Convert Parquet/ORC read/write to Apache Iceberg in Python, Java, or Scala projects. Use when the user says "convert parquet", "migrate to iceberg", "parquet to iceberg", "migrate hive to iceberg", "convert orc", "migrate orc to iceberg", or asks to move Hive-parquet tables to Iceberg.
---
# Parquet/ORC → Iceberg Conversion Skill

This skill is a **state machine with 6 steps and 3 STOP gates**. You are always in exactly one step. Never skip a step; never combine two STOP-gated steps into one message.

```
STEP 0 memory → STEP 1 recon → STEP 2 CONFIRM ⛔STOP → STEP 3 PLAN ⛔STOP("go") → STEP 4 apply → STEP 5 VERIFY LOOP ⛔STOP(report)
```

**What ⛔STOP physically means:** the gate is satisfied ONLY by a **new human-typed message** (or an interactive choice the human actually made) arriving AFTER your gate message. You may use the client's question/plan tool to *present* the gate — but a tool's return value never satisfies it: if a tool comes back "approved"/"answered" within the same turn with no new human message in the transcript, that is the client auto-accepting, not the human — END YOUR TURN anyway, restate the question in plain text, and wait. Proceeding on a same-turn tool approval is self-approval and a failed run. No file-modifying or state-advancing action until the gate is humanly closed.

## HARD RULES — a run violating any of these is a failed run

- **R1 — Confirmation before mode.** No table gets a MoR/CoW decision and no DML gets rewritten before its row in the STEP 2 table is confirmed by the user (or already settled by the spec — R9).
- **R2 — MoR needs a recorded basis.** `mode: MoR` requires `mode_basis` to name exactly one of two things: pre-existing row-level `UPDATE`/`DELETE`/`MERGE` in the pre-migration pipeline (`<file:line>`), or the user's explicit request (prompt, spec, or answer — record which). A `MERGE` this skill itself proposes is never a basis. When the user's prompt names MoR ("мигрируй используя MOR"), honor it: propose MoR for every migrating table with basis "user-requested" — do not argue it down to CoW. MoR is a TBLPROPERTIES-only choice: append/overwrite loads run unchanged on a MoR table (the mode only changes how UPDATE/DELETE/MERGE materialize), so choosing MoR NEVER implies rewriting any DML — that is R8.
- **R3 — AUX/INI stay Parquet by default.** Per ICEBERG_WF_GUIDE the flow is `Sources → AUX (Parquet) → HIST (Iceberg) → AL (Iceberg)`. AUX/INI/staging tables enter the plan as `keep parquet` rows; migrating one requires an explicit user "yes" recorded in decisions.yml.
- **R4 — Never invent values.** Every entity_id, Spark conf key, TBLPROPERTIES value, wf name, YAML anchor must be COPIED from this project, from a sibling project (then ADAPTED: prefixes/schemas/paths of THIS project), or from the guides. A value you cannot point to a source for = ask the user. This includes Spark conf keys: the iceberg conf param contains EXACTLY the 3 flags from the guide plus only sibling-verified extras — do not compose plausible-looking `spark.sql.catalog.*` keys from memory (procedure options like `partial-progress` and table properties like `commit.retry.*` are NOT Spark confs).
- **R5 — No table migrates without maintenance.** Every table whose final mode is Iceberg (MoR or CoW) gets `exp_iceberg_<table>.sql` + `upd_iceberg_<table>.sql` AND an entry in the maintenance wf — in the same run. If you migrate 8 tables, maintenance covers 8, not 5.
- **R6 — Every reader is covered by the conf.** Every wf in ctl.yml that reads OR writes any migrated table must resolve to the iceberg conf — either directly in its `spark_submit_cmd` or via a shared `ssc_*` param it references (see STEP 4.2; the shared param is preferred). There is NO separate "merge conf": a `--conf` key that exists in neither the sibling nor the guide is invented (R4). Find readers by grepping ctl.yml/DML for the table name — not from memory.
- **R7 — Done means verified.** "Done" may only be declared by STEP 5 after a probe pass with zero failures. One grep is not verification.
- **R8 — Mode ≠ DML.** No working load script is rewritten to `MERGE INTO` unless BOTH: (a) the file was explicitly listed as a numbered item in the STEP 2 gap questions and the user approved that exact item, and (b) the table's confirmed write_pattern is `upsert` with a verified unique key. Approving an empty list approves nothing — if no file qualifies, the question is not asked at all. `scd_versioned` tables never get a single-pass MERGE — a matched key would close its old version while the new version can never be inserted (MATCHED and NOT MATCHED are mutually exclusive → silent loss of current state); their load stays INSERT/append, or uses the two-statement pattern in ICEBERG_WF_GUIDE § "SCD2-таблицы и Iceberg" only if the user explicitly asks for version-closing.
- **R9 — Never ask what is already answered.** Before every question, check the decision file and the spec documents read in STEP 0. Asking something they already answer is a failed run. Each decision has exactly ONE owner:
  - **the human** (via spec / previous answers / this session's request) — which tables and layers are in scope, storage mode, whether loads may be rewritten, which project is the reference;
  - **the code** — write_pattern, unique key, partitioning, existing readers;
  - **the reference project** — conf keys, codec, file size, wf structure;
  - **the project registry** — entity_id; **S2T** — table comments.

  You may never ask about the last four groups: derive them, record them with their source, and SHOW them for correction. A question is legitimate only when the value is the human's to give AND absent from every input. Always print the tally first: `reused N · derived M · questions K`.

## STEP 0 — memory (first action after the announce)

Announce verbatim:

> "I'm using the iceberg-migration-skill (v3 state machine)."
> "Order: recon → per-table confirmation (STOP) → plan (STOP, wait for go) → apply → verify loop. I read any existing spec (OpenSpec change / decision file) first and ask only about what it leaves open; decisions persist in ./.iceberg-migration/decisions.yml."

Then collect every decision that ALREADY exists, from these three sources in order — first hit wins per field, and everything found here is DECIDED, never re-asked (R9):

```bash
# 1. decisions from earlier runs — look in the datamart dir AND the repo root, they differ between clients
find . -maxdepth 4 -path '*/.iceberg-migration/decisions.yml' 2>/dev/null
# 2. spec-driven project? openspec/ may sit at the repo root OR inside the datamart — search, don't assume:
find . -maxdepth 4 -type d -name changes -path '*openspec*' 2>/dev/null
```

**Never conclude "no spec" from one `ls` of one directory** — run the search above from wherever you are, and if it comes back empty, say so explicitly in STEP 2 ("спека не найдена, работаю по вашему сообщению") so the human can point you at it. If the user named a path, use it verbatim and skip the search.

**2 — OpenSpec (if present).** If several changes exist, pick the one whose name and content match this task (domain, datamart); if two plausibly match, do not guess — list them in STEP 2 and ask which. These are prose documents with stable headings, not key-value files. Read `design.md` sections *Goals / Non-Goals*, *Decisions*, *Open Questions*, plus `proposal.md`, and pull out only what they actually state: which tables/layers are in scope, storage mode, reference project, explicit prohibitions ("не трогать загрузки"), maintenance schedule. Record each with its origin, e.g. `by: openspec design.md §Decisions`. **What the spec does not say is a gap, not permission to decide for the user** — a gap becomes a question in STEP 2, an invention is a failed run (R4). Quote back what you extracted in STEP 2 so the human can correct a misreading.

**3 — the user's message in this session** (e.g. "мигрируй в MoR") — same status as the spec.

Merge all three into the ONE file below — it is both the specification and the record of decisions; there is no second document. Create it if missing and write to it immediately after each answer, not in a batch. Every field carries `by:` (who decided) so nothing is ever asked twice:

```yaml
# maintained by iceberg-migration-skill — spec AND record of decisions, one file
# commit it together with the migration
version: 3
project: <datamart>
domain: <domain>
spec_source: openspec/changes/<change>/   # or "none — decided in session"
# by: human | openspec <file §section> | code <file:line> | reference <proj file:line> | registry | default
global:
  reference_project: {value: <proj>, by: openspec design.md §Decisions}
  iceberg_conf_param: {value: <name>, by: reference <proj>/mart.yml:227}
  maintenance_wf: {value: <name>, by: default}      # adapted to THIS project's prefix — no sibling literals
  compression_codec: {value: <v>, by: reference <proj>/ddl/x.sql:75}
  rewrite_loads: {value: false, by: default}        # R8 — true only if the human said so explicitly
tables:
  <schema>.<table>:
    action: migrate|skip                # scope — human/openspec only
    layer: {value: AUX|INI|HIST|AL, by: code}       # AUX/INI default keep-parquet (R3)
    write_pattern: {value: append|overwrite|upsert|scd_versioned, by: code <file:line>}
    unique_key: {value: [..], by: code}  # of a PHYSICAL row; scd_versioned MUST include the version column
    preexisting_mutations: {value: none|<file:line>, by: code}
    mode: {value: keep-parquet|CoW|MoR, by: human}
    mode_basis: "user-requested (prompt|spec|answer)"|"pre-existing MERGE <file:line>"   # never a MERGE this skill added (R2)
    merge_rewrite_approved: true|false|n/a  # true only per individually approved file (R8)
    entity_id: {value: <id>, by: registry ctl.yml:<line>}   # never invented (R4)
    status: planned|applied|verified
open_questions: []                      # gaps only — the sole legitimate content of STEP 2 questions
verify_log: []                          # appended by STEP 5, one entry per pass
```

A field that already has a value — from ANY source — is settled: use it, show it, never ask about it (R9).

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

## STEP 2 — CONFIRMATION ⛔ STOP — this message contains ONLY the block below and ends your turn

**This is not an interview — it is a filled-in table shown for correction.** Every cell already has a value and an origin: taken from the spec/decision file, derived from the code, copied from the reference project, or read from the registry. Deriving and showing is always better than asking (R9). The user's job is to spot a wrong cell, not to supply data.

```
### Решения по таблицам — поправьте, если что-то не так
Источник спеки: openspec/changes/<change>/  ·  переиспользовано N · выведено M · вопросов K

| # | Таблица | Слой | Паттерн записи | Ключ строки | Были ли UPD/DEL/MERGE | Решение | Откуда |
|---|---------|------|----------------|-------------|----------------------|---------|--------|
| 1 | aux.t_x | AUX | версионная (ctl_validfrom/to) | (бк…, ctl_validfrom) | нет | остаётся Parquet | спека §Non-Goals |
| 2 | pa.t_y  | AL  | перезапись | (epk_id) | нет | Iceberg MoR, загрузка без изменений | спека §Decisions |

Из спеки понял так: <2–4 строки — состав, режим, эталонный проект, запреты>.
Если понял неверно — поправьте, это важнее остального.

Расхождения спека/код: <строкой каждое — спека говорит X, в коде Y; действую по спеке, просто предупреждаю>  (или "нет")
```

Then, and only then, the questions — **gaps only**, numbered, each with your recommendation so a one-word answer suffices:

- A gap is a value that is the human's to decide (scope, mode, permission to rewrite a load) AND is absent from spec, decision file and this session. Everything else you derive — never ask.
- MERGE rewrites: list the qualifying files individually with a Y/N each. If none qualify, **omit the question entirely** — do not ask for confirmation that the list is empty (an empty list approves nothing, R8).
- Values you could not source from the reference project are **not a question** — they are a finding: report them as a blocker and stop, do not invent (R4).
- If there are no gaps, write exactly: `Вопросов нет. Поправьте таблицу, если что-то не так, иначе напишите «дальше».`

Record every answer — and every derived value with its origin — into the decision file, then go to STEP 3.

### Draft mode — "набросай спеку, ничего не меняя"

**This mode is optional — the normal run already shows every decision at STEP 2 before touching anything.** Use it only when the user explicitly asks for a draft/spec ("набросай спеку", "подготовь решения, ничего не меняй"), typically to review and edit the decisions offline before the real run.

Then run STEP 0–1 and **write only `.iceberg-migration/decisions.yml`**: every field filled and marked with its origin, every gap left as `TODO(human): <вопрос>` under `open_questions`. Touch no project file — a change to any file under `sql/` or `wf/` in this mode is a failed run. Finish with the tally and the list of TODOs. The human edits that file offline; the next run starts from it and asks nothing.

## STEP 3 — PLAN ⛔ STOP — end your turn; apply starts only after the human types "go"

If the client forces a plan tool (ExitPlanMode etc.), you may call it to present the plan — but its return value is NOT "go". "User approved the plan" arriving in the same turn with no new human message = client auto-accept: end the turn, write "Ожидаю ваше 'go' в чате", and wait. Apply starts only after the human's own message.

The plan message is INVALID unless it physically contains, in this order: (a) the confirmed table from STEP 2, (b) the pasted current content of decisions.yml, (c) the sections below. If (a) or (b) is missing you are still in STEP 2.

```
### Migration plan — <project>/<domain>
[confirmed table from STEP 2 here]
[--- decisions.yml (current) ---
<paste file>
---]

Files to change:
- mart.yml — add <iceberg_conf_param> (exactly the 3 guide flags + extras copied VERBATIM from the sibling, each with file:line source; no separate "merge conf" exists — R4/R6), ssc/wf vars for maintenance
- ctl.yml — cover THESE reader/writer wf (full list from recon step 3): <list — every wf touching every migrated table, R6>; coverage mechanism: <shared ssc_<domain> param patched once [preferred] / per-wf patches — name which wf gets which>
- ctl.yml — add maintenance wf <name> covering THESE tables: <every non-keep-parquet row, R5>
- sql/ddl/… — per non-keep-parquet row: USING iceberg + FULL TBLPROPERTIES per reference table (all 11 defaults; fewer only if a row-specific decision says so)
- sql/dml/exp_iceberg_<t>.sql + upd_iceberg_<t>.sql — per non-keep-parquet row; where-predicate = THAT table's own PARTITIONED BY column; unpartitioned → binpack, no where
- .iceberg-migration/verify.sh — WRITE IT NOW: the whole STEP 5 probe block with every <placeholder> substituted and each `# P<n>` header turned into `echo "=== P<n> ==="`; STEP 5 does nothing but run this file
- (only for files individually approved in Q3 — R8) <file> → MERGE rewrite

Out of scope (and why): <keep-parquet rows; untouched wf>
Reply "go" to apply, or list changes.
```

## STEP 4 — apply (order matters)

**Re-read this SKILL.md file now, before the first edit** (and again before STEP 5). By this point a long session has likely pushed it out of your working context — the recon and confirmation happened many tool calls ago. Re-reading costs one file read; skipping it is how apply-phase runs drift back to defaults.

1. **mart.yml** — conf params. The iceberg param = the 3 mandatory flags (extensions / SparkSessionCatalog / catalog.type=hive — exact strings in ICEBERG_WF_GUIDE) + only extras copied VERBATIM from the sibling (R4 — record the sibling file:line in decisions.yml; retyping a block with different numbers, e.g. your own dynamic_allocation values, is inventing). No merge conf exists. Shared param, referenced as `{{mart.<param>}}` — never inline the flags per-wf.
2. **ctl.yml, existing wf** — cover every wf on the recon reader/writer list. **Check first whether the readers share a `ssc_<domain>`-style param** (e.g. several wf all build their command from `{{mart.ssc_agr_bond}}`): if so, add `{{<iceberg_conf_param>}}` **inside that shared param once** — it covers every wf referencing it and can't miss a reader; patch individual wf entries only for readers outside the shared param.
3. **DDL** — per table: `USING iceberg`, `PARTITIONED BY`, and this full block — sourcing rules for each `<value>` are in **ICEBERG_WF_GUIDE.md § "DDL: full TBLPROPERTIES template"** (codec from sibling; file-size agreed with the compaction script; bloom only when justified per table, else omit; comment verbatim from S2T). **Partition column move (mandatory):** Hive declares partition columns ONLY inside `partitioned by (col TYPE comment …)` — they are absent from the column list. Iceberg requires them IN the column list: append each partition column to the schema as the last column (same name, type AS-IS, same comment), and leave `PARTITIONED BY (col)` with the bare name, no type. A converted DDL whose `PARTITIONED BY` names a column missing from the column list fails `CREATE TABLE` — this is the most common conversion crash (probe P4 checks it):

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
5. **ctl.yml, maintenance wf** — **first prove the oozie application the wf will point at exists in THIS project's deploy set**: `ls src/main/resources/wf/oozie/` and `grep -n "hdfs_care" src/main/resources/devops/devops.json`. If recon said "no hdfs_care in this project", the app is NOT deployed — a wf pointing at `{{devops.datamart_path_app}}/hdfs_care` anyway can never run. In that case STOP and ask the user: copy the sibling's oozie app into this project's deploy set (name the exact files/devops.json entries to add), or point at an app that already exists here. Then **mirror the sibling's maintenance-wf entry structurally**: same set of fields (entity/publish ids, anchors, cron, params) with every value adapted to THIS project — a field the sibling has and yours lacks is a finding, an id or anchor the sibling has that appears nowhere in this project must be asked for, never invented. Name adapted to this project's prefix; entries for EVERY migrated table; params `app.sql.service_date_from` / `safe_days` / `retain_snapshots` defined and format-compatible with the partition values (see 4.4).
6. **Gherkin/S2T** if the project has them.
7. Update each table's `status: applied` in decisions.yml.

⚠ DML edits are governed by R8: a load becomes MERGE only if its file was an individually approved Q3 item AND its table is a confirmed `upsert`. `scd_versioned` → the load stays INSERT/append even on a MoR table; if the user explicitly asked for version-closing, copy the two-statement pattern from ICEBERG_WF_GUIDE § "SCD2-таблицы и Iceberg" — never a single-pass MERGE (matched keys lose their current version forever), and never `alias.current_timestamp()` (invalid Spark SQL — always bare `current_timestamp`).

## STEP 5 — VERIFY → REPAIR ⛔ loop, max 3 passes

At STEP 3 you wrote this block — names substituted, `# P<n>` headers turned into `echo "=== P<n> ==="` — to `.iceberg-migration/verify.sh`. If it is missing, write it now. **Each pass = ONE execution:** `bash .iceberg-migration/verify.sh > .iceberg-migration/verify_pass<N>.log 2>&1`, then paste the log and judge every probe against its rule. Manually retyped or narrated probe results are not evidence — a pass with no script log is a FAILED pass. Collect failures, append `{pass, failed, fixed}` to `verify_log`, fix, re-run the WHOLE script (a fix can break a previously green probe). **A pass that contains any fix is a FAILED pass by definition — after fixing, increment the pass counter and restart from P1; only a pass with zero failures AND zero fixes can be the final one.** The final report lists every probe P1–P10 by its number with the script's raw output pasted under it — a probe with no raw output, a renumbered probe, or a skipped probe counts as FAILED. Failures after pass 3 → report them as OPEN BLOCKERS, not done.

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

# P4 predicate vs partition + partition column in schema — THREE commands per table, raw outputs pasted:
grep -n "PARTITIONED BY" src/main/resources/sql/ddl/<domain>/<table>.sql   # ground truth — never state partitioning from memory
grep -c "<part_col>" src/main/resources/sql/ddl/<domain>/<table>.sql       # must be ≥2 (column list + PARTITIONED BY); =1 → column exists only in PARTITIONED BY, CREATE TABLE will fail (STEP 4.3 partition move skipped)
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

# P6 conf keys are sourced — one grep PER --conf key your edits added (take the list from your diff):
for k in <conf_key_1> <conf_key_2> ...; do echo "-- $k"; grep -rh "$k" <sibling>/src/main/resources/wf/ctl/mart.yml ICEBERG_WF_GUIDE.md | head -2; done
# zero hits for a key = invented key (R4) = FAILED; hits found → also compare the VALUES against the source line — silently changed numbers = FAILED
# P7 no sibling literals + wf integrity — commands, not judgement:
grep -rn "<sibling_prefix>" src/main/resources/wf/ctl/                                        # must be empty
ls src/main/resources/wf/oozie/; grep -n "hdfs_care\|<oozie_app>" src/main/resources/devops/devops.json   # the app the new wf points at must exist/deploy HERE; recon said "not found" + wf still points there = FAILED
grep -n "&wfDefaultInfo\|&wfParamsDefault\|&scheduleParams" src/main/resources/wf/ctl/ctl.yml # every anchor the new wf references must be DEFINED in this file
# P8 entity_id provenance — every entity.id you added appears elsewhere: grep -rn "<id>" src/main/resources/ beyond your edit.
#    ALSO cross-check decisions.yml: every entity_id recorded there must match the entity list in ctl.yml for that exact table
#    (grep -n "<table>" ctl.yml entity section) — a memory file with fabricated ids is a failed probe even if the wf files are correct; fix the record
# P9 TBLPROPERTIES completeness — ONLY migrated tables' DDL (keep-parquet files must NOT be counted — they'd false-fail);
# count quoted property KEYS, not the word "comment" anywhere (per-column comments would inflate the count):
for t in <migrated tables>; do grep -cE "'(write\.[a-z.-]+|format-version|comment)'" src/main/resources/sql/ddl/<domain>/*$t*.sql; done   # ≥ 10 each (11 props; bloom only when justified) unless a recorded row decision says fewer
# P10 MoR justification + R8 — one command, judge from raw output:
grep -B4 -A6 "mode: MoR" .iceberg-migration/decisions.yml
# FAILED if: any MoR row's mode_basis is neither "user-requested…" nor "pre-existing … <file:line>"; any row has merge_rewrite_approved: true
# for a file that was never an explicit numbered Q3 item; any scd_versioned table's load DML now contains a single-pass MERGE:
grep -l "MERGE INTO" src/main/resources/sql/dml/<domain>/<scd_load_files>   # a hit on a scd_versioned load = FAILED (R8) unless it is the two-statement version-closing pattern the user explicitly requested
```

Final report = summary + last verify pass + decisions.yml path. Nothing else counts as done.

## Reference

Details live in the guides — TBLPROPERTIES reference table and DDL/DML/wf templates: ICEBERG_WF_GUIDE.md + reference.md; S2T: S2T_GUIDE.md; worked examples: examples.md. When a template and this file disagree, the guides win for file contents; this file wins for process.
