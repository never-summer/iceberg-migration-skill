---
name: iceberg-migration-skill
description: Convert Parquet/ORC read/write to Apache Iceberg in Python, Java, or Scala projects. Use when the user says "convert parquet", "migrate to iceberg", "parquet to iceberg", "migrate hive to iceberg", "convert orc", "migrate orc to iceberg", or asks to move Hive-parquet tables to Iceberg.
---
# Parquet/ORC → Iceberg Conversion Skill

**Announce at start (verbatim, two lines):**

> "I'm using the iceberg-migration-skill to convert this project."
> "I will first do a read-only reconnaissance pass (read the four guides, scan for I/O sites + pipeline anti-patterns, locate any existing maintenance wf and Iceberg conf), then run a short per-table interview (write pattern / real key / MoR-vs-CoW / which layers to migrate), then present a migration plan and wait for your `go` before modifying files. I reuse any answers saved in `./.iceberg-migration/decisions.yml` from a previous run and only re-ask about what changed."

## Recommended order (skip only with good reason)

These steps are recommendations, not enforced gates. They exist because past runs that skipped them produced shallow mechanical Parquet→Iceberg rewrites and missed the MERGE / `.changes` / MoR opportunities the user actually wants.

1. **Read the four guides first** — [S2T_GUIDE.md](./S2T_GUIDE.md), [ICEBERG_WF_GUIDE.md](./ICEBERG_WF_GUIDE.md), [reference.md](./reference.md), [examples.md](./examples.md). They contain project-shape assumptions the rest of this skill relies on. Reading DDLs / `pom.xml` / `ctl.yml` before these is fine if it helps, but the guides should be on your context by the time you write the migration plan.
2. **Do reconnaissance before proposing changes** — surface I/O sites, scan for pipeline anti-patterns, locate any existing maintenance wf, locate any existing Iceberg Spark conf YAML parameter. Concrete commands are below in the "Reconnaissance" section.
3. **Present a migration plan and wait for the user's `go`** before editing files. The plan template lower in this document is a useful structure; deviate when it doesn't fit the project.

If you catch yourself printing an "🔍 Анализ" / "Tables summary" block before the user has the plan in front of them, you skipped step 3 — back up.

## Persistent decisions & re-runs (memory + retry loop)

This migration is rarely one-shot: the same project gets re-run after review, a mode gets flipped, a table gets added. **Do not re-interrogate the user for answers they already gave.** The skill keeps a small state file at the project root:

```
./.iceberg-migration/decisions.yml
```

**At the very start (before the interview and before the plan):** read this file if it exists.

```bash
cat ./.iceberg-migration/decisions.yml 2>/dev/null || echo "no prior decisions — first run"
```

It records, per run and per table, everything the user already decided, so a re-run asks only about *deltas* (new tables, changed requirements, checks that failed last time). Suggested shape — extend as needed, don't treat as rigid schema:

```yaml
project: custom_blago_fc
updated: 2026-07-08
globals:                       # asked once, reused across tables
  iceberg_conf_param: spark_submit_cmd_iceberg   # reuse, never rename
  maintenance_wf: wf_schema_hdfs_care
  compression_codec: zstd      # confirmed from sibling custom_blago_al_wms
  target_file_size_bytes: 134217728
  distribution_mode: none
  sibling_reference: custom_blago_al_wms/wf/ctl/ctl.yml:217-326
tables:
  custom_blago_fc_aux.t_bond:
    layer: AUX
    write_pattern: scd_versioned     # append/overwrite/upsert/scd_versioned
    keep_parquet: true               # AUX stays parquet — do NOT migrate
    mode: n/a
  custom_blago_fc.t_agr_bond:
    layer: AL
    write_pattern: upsert
    unique_key: [epk_id]             # the REAL key, incl. version col if SCD
    mode: MoR
    mode_basis: "pre-existing MERGE: sql/dml/pa/t_agr_bond.sql:442"   # or "user-specified" — never a MERGE this skill itself added
    entity_id: 920551014             # COPIED from ctl.yml / 1_ctl_entities.yml — never invented; if absent there, ask
    merge_rewrite_approved: true
verify_log:                    # appended by the Verify→Repair loop
  - run: 1
    failures: ["service_date_from undefined", "rewrite predicate part_report_dt not a partition of t_agr_bond"]
  - run: 2
    failures: []
```

**Rules:**
- Before asking any interview question, check whether `decisions.yml` already answers it. If yes and nothing upstream changed, use the saved answer and say so ("reusing prior decision: `t_bond` = AUX/keep-parquet"). Only ask when the file is silent, stale, or the user's new request contradicts it.
- **Write/update the file at two moments:** right after the interview (record every answer), and after each Verify→Repair pass (append the `verify_log` entry). Commit it with the migration — it is the audit trail of why each table got its mode/key.
- Treat it as advisory memory, not law: if the user says something new that conflicts, the new instruction wins and you overwrite the stale entry (and note the change).
- **`entity_id` values are copied, never invented.** Every ID must be found in `ctl.yml` / `1_ctl_entities.yml`; an ID that appears nowhere else in the project is a fabrication and a defect. If no ID exists for a new wf entry, ask the user and record the answer.
- **Values taken from a sibling project are adapted before they are recorded or written** — prefixes, schema names, wf names, paths must match *this* project. Record the sibling origin (`# confirmed from sibling …`) so the adaptation is auditable. A literal like `wms-al-schema-hdfs-care` landing inside a non-`wms-al` project is exactly the leak this rule exists to stop.

The retry mechanics that consume `verify_log` are the **Verify → Repair loop** at the end of "Apply changes" below.

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

   # Target tables already on Iceberg (re-migration / mode-switch / property-audit case):
   grep -rn 'USING iceberg' <project>/src/main/resources/sql/ddl/ 2>/dev/null
   ```

   This is intentionally noisier than the old Python detector — the detector parsed AST and resolved string-formatted paths. With raw search you may catch a `parquet` mentioned in a comment or a log string; read each hit before adding it to the worklist. If the last command finds the target table(s) already on `USING iceberg`, stop and read "MANDATORY — if the target table already says `USING iceberg`..." below before drafting the plan — this is a property-audit task, not a fresh migration, and changes what the plan needs to cover.
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
- [ ] Checked whether any target table already has `USING iceberg`           y/n
      (paste grep result). If yes, audited its full TBLPROPERTIES against
      the Property reference table — see row below.
- [ ] Read `./.iceberg-migration/decisions.yml` (prior-run memory) and         y/n
      reused saved answers instead of re-asking — see "Persistent decisions".
- [ ] Ran the per-table clarifying interview (layer; write pattern            y/n
      append/overwrite/upsert/SCD; REAL unique key incl. version col;
      genuine pre-existing row-level UPDATE/DELETE; permission to rewrite
      SELECT/INSERT→MERGE) and recorded answers — see MANDATORY interview
      block. For any table you defaulted MoR, name the answer that justifies
      it (must NOT be a MERGE this skill itself introduced).

If any item is `n`, consider whether you should fill it in before presenting the plan — it likely affects the recommendation quality. The user can also tell you to skip an item explicitly (e.g. "this project has no S2T, skip that row").

### Reference template (from Phase A.7 sibling recon)
- Reference: `<sibling_project>/wf/ctl/<file>.yml:<line-range>` for table `<t>`
- (or: "No sibling Iceberg ctl found — falling back to ICEBERG_WF_GUIDE generic template (less anchored, may need iteration)")

### Tables to migrate (from worklist + S2T)
| # | Source | Target Iceberg | Format | Code sites | DDL file | entity_id | Mode (MoR/CoW) | Compaction template | Already on Iceberg? |
|---|---|---|---|---|---|---|---|---|---|
| 1 | ... | ns.t | parquet | 7 | ddl/x.sql | 920... | MoR | MoR-aware (see ICEBERG_WF_GUIDE "Полный цикл обслуживания для MoR") | No |
| 2 | ... | ns.t2 | iceberg (already) | — | ddl/y.sql | 920... | CoW→MoR | MoR-aware | **Yes — 5/11 properties present, see audit below** |
...

For any row marked "Already on Iceberg" — list the existing/missing properties explicitly, per the MANDATORY rule above: `"<table>: existing TBLPROPERTIES has N/11 properties from the reference table — bringing to full set while applying the requested change."` Do not silently skip this row just because the user's request named only one property (e.g. the mode).

**Mode selection — user input is ground truth.**

If the user has explicitly named a mode (`CoW`, `Copy-on-Write`, `MoR`, `merge-on-read`) for the migration, generate DDL with **that** mode for the affected tables. Do **not** override based on the DML heuristic. If you believe the user's choice is operationally risky (e.g. MoR without a MoR-aware compaction template, or CoW under heavy row-level mutations causing rewrite amplification) — flag the concern in the plan and **ask**, do not silently switch.

**Default mode (used only when the user has not specified) — and only after the per-table interview below.** Pick MoR **only** when the table is a *true upsert/delete target*: it receives row-level `UPDATE` / `DELETE` / `MERGE` **that already existed in the pre-migration pipeline**, against its own current rows. 

⚠ **Do NOT treat a `MERGE INTO` that *this skill* proposes** (from a FULL OUTER JOIN / `*_inc.sql` rewrite — see the anti-pattern pass) **as evidence for MoR.** That is circular — the skill would be manufacturing its own justification, and it is exactly how append/SCD tables get wrongly flipped to MoR. Base the decision on the table's *pre-existing* write pattern, confirmed in the interview, not on DML the skill is about to introduce.

Before defaulting to MoR, confirm the table is NOT one of these MoR-inappropriate shapes (this is an interview question, not a grep you can trust). **This list is the canon** — the SCD warning in the pipeline-analysis section and interview question 2 restate it for emphasis; if you ever edit the shapes, edit them here and keep the other two as pointers:

- **append-only / SCD-versioned** — columns like `ctl_validfrom` / `ctl_validto` / `ctl_pa_loading`, partition like `part_ctl_validfrom` / `ctl_loading`. Every load appends a **new version** of the row; there are no row-level updates. MoR here adds read overhead for zero write benefit, **and** rewriting the load into `MERGE INTO <business_key>` collapses history (see the SCD warning under the pipeline-analysis block). Keep it **CoW/append**. If it is an **AUX** staging table, per the ICEBERG_WF_GUIDE data-flow architecture (`AUX = Parquet`) it usually **stays Parquet — do not migrate it at all without asking**.
- **full-reload / `INSERT OVERWRITE`** target — rebuilt wholesale each run. CoW, not MoR.

Otherwise (real upsert target: pre-existing `UPDATE` / `DELETE FROM` / `MERGE` against current rows) → MoR. The grep `grep -ril 'UPDATE \|DELETE FROM\|MERGE INTO' src/main/resources/sql/dml/` is a *starting signal only* — every hit must be read and classified in the interview (is it a genuine mutation of current rows, or an append/overwrite/version-insert?). State the basis for each row in the plan table: "user-specified", "interview: <shape>", or which **pre-existing** DML mutation triggered the default.

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
- mart.yml — define `<conf_param_name>` (if not reusing); define `app.sql.safe_days` / `app.sql.retain_snapshots` / `app.sql.service_date_from` (or equivalent) and `ssc_schema_hdfs_care` if not already present — the compaction scripts reference these and fail without them
- ctl.yml — add `<wf_name>` per-table maintenance entry **with the `spark_driver_extraJavaOptions__hdfs_care_*` params actually pointing at the new `exp_iceberg_<table>.sql`/`upd_iceberg_<table>.sql` paths** (not just creating the scripts), patch existing wf reading these tables to add `{{mart.<conf_param_name>}}`
- sql/ddl/<layer>/<table>.sql — for tables not yet on Iceberg: switch STORED AS PARQUET → USING iceberg + the full TBLPROPERTIES block. For tables already on Iceberg (flagged in the table above): apply the requested change AND backfill any missing "Always"/"Default" property from the Property reference table in the same edit — this covers **every** table row above, including upstream/staging layers that feed the target table
- sql/dml/exp_iceberg_<table>.sql / upd_iceberg_<table>.sql — new compaction scripts
- s2t/qaapi/*.feature — add Gherkin DDL scenarios for new tables
- (If anti-pattern accepted) sql/dml/<table>_inc.sql — replace with MERGE INTO + `.changes` consumer

### Open questions for the user (BLOCK on these)
- Per-table interview rows not yet confirmed (layer / write pattern / unique key) — see the interview table above; the mode follows the confirmed **write pattern** (upsert → MoR candidate; append / scd_versioned / overwrite → CoW or keep-parquet), never the mere presence of a MERGE in the DML.
- If you've already specified a mode, it applies to the tables that qualify (pre-existing row-level mutations per the interview); append/SCD/AUX rows are flagged separately, not silently flipped.
- Accept the MERGE / `.changes` rewrite for `t_X_inc.sql`? (Y/N)
- Maintenance wf name to use (if not reusing)?
- Iceberg conf YAML param name (if not reusing)?

---
Reply **"go"** to apply this plan as-is. Or list changes/exclusions.
```

### Apply changes (writes) — only after explicit user approval

Execute in this fixed order so reviews stay sane. **Step 2 is split into 2a and 5** — the new maintenance-wf entry (5) has to reference the compaction scripts created in step 4, so it cannot land before they exist. Do not try to do "all of ctl.yml" in one pass before step 4 — that ordering is impossible to satisfy and is the single most common reason this migration gets reported done with the maintenance wf never actually wired.

1. `mart.yml` — define / reuse the iceberg conf YAML param, plus the `app.sql.safe_days` / `app.sql.retain_snapshots` / `app.sql.service_date_from` (or equivalent) and `ssc_schema_hdfs_care` params that the compaction scripts in step 4 will need — define these now so step 5 isn't blocked later.
2. `wf/ctl/*.yml` (pass 2a) — patch `spark_submit_cmd` on every **existing** wf that reads/writes a migrated table to also reference `{{mart.<conf>}}` (no inlining). This part has no dependency on step 4 and can run now.
3. `sql/ddl/<layer>/<table>.sql` — switch to `USING iceberg` + the **full** TBLPROPERTIES block from the "Property reference" table in Step 6 below — not just `format-version` + the three `write.*.mode` lines.
4. `sql/dml/exp_iceberg_<table>.sql` and `upd_iceberg_<table>.sql` — compaction scripts per ICEBERG_WF_GUIDE templates.
5. `wf/ctl/*.yml` (pass 2b) — **immediately after step 4, same pass, before moving to step 6** — add the maintenance-wf entry (new or reused, e.g. `wf_schema_hdfs_care`) with the `spark_driver_extraJavaOptions__hdfs_care_*` params pointing at the step-4 scripts by path. If your session is at risk of running out of turns/budget, do this step before Gherkin/anti-pattern work, not after — a migration that has DDL+DML but no wf wiring is not a partial success, it's broken (queries work, nothing ever compacts or expires snapshots). See Step 6.2's self-check below for the four conditions that must all be true before you consider this landed.
6. `s2t/qaapi/*.feature` — Gherkin scenarios.
7. (If approved) Replace `*_inc.sql` with MERGE INTO + retire `wf_*_inc`.
8. (Optional) Write a small `iceberg-runbook/` directory by hand: one `phase1_add_files.sql` and one `phase2_rewrite.sql` per migrated table, following the templates in [reference.md § Phased migration runbook](./reference.md#phased-migration-runbook). This used to be auto-generated; in the markdown-only skill you compose them from the templates and the per-table info already captured in the plan.

### Verify → Repair loop (run yourself, up to 3 passes — do not hand gaps to the user)

Migrations here are almost never right on the first apply, so treat verification as a **bounded self-correcting loop**, not a one-shot reminder. After applying, print a final summary (changed / created / retired), then run the probes below yourself — *actually run the commands*, don't describe them. Each probe is a concrete, checkable condition; collect the failures, append them to `verify_log` in `./.iceberg-migration/decisions.yml`, fix them, and re-run **the whole probe set, not just the failed probe** — a fix can break a probe that previously passed (e.g. renaming a wf breaks the wiring probe). Repeat until all pass or you hit 3 passes; if still failing after 3, stop and report the remaining failures explicitly (with the `verify_log`) rather than reporting "done".

**Probes:**

1. **No residual Parquet on migrated tables** — re-run the recon `rg`/`grep` scoped to **every** row of the Phase B plan table, not a subset. Grep the whole project, then cross-check the hit list against each plan row one by one (the classic miss is converting the `aux`/`ini` staging layers but leaving the `pa`-layer aggregate that was the actual target). Tables deliberately kept Parquet (AUX/INI per the interview) must still show `STORED AS PARQUET` — verify they were *not* touched.
2. **Compaction wiring actually landed** — for every MoR/CoW table that needs a maintenance wf, `grep -rn '<table>' src/main/resources/wf/ctl/` must show the table inside a `spark_driver_extraJavaOptions__hdfs_care_*` param (pointing at its `exp_iceberg_<table>.sql` / `upd_iceberg_<table>.sql` by path), not merely inside DDL/DML you wrote. A `.sql` file with no wf reference is inert.
3. **Compaction params resolve** — every `${app.sql.*}` referenced by the maintenance scripts (`app.sql.safe_days`, `app.sql.retain_snapshots`, **`app.sql.service_date_from`**) resolves to a definition reachable by the wf: `grep -rn 'app.sql.service_date_from\|app.sql.safe_days\|app.sql.retain_snapshots\|date_achive_from' src/main/resources/wf/`. A missing `service_date_from` is a top-of-list failure — the wf runs and dies immediately on the undefined parameter (looks complete because the `.sql` exists).
4. **Rewrite predicate matches the real partition** — the `where => '<col> >= ${app.sql.service_date_from}'` in each `upd_iceberg_<table>.sql` must reference a column that is **actually a partition of that table**. Do not carry a template's `part_report_dt` onto a table partitioned by `part_ctl_validfrom` — a wrong predicate makes `rewrite_data_files` error or silently no-op, so MoR delete files never compact (the silent read regression).
5. **Three Iceberg conf flags present** — every wf touching a migrated table references the shared iceberg-conf YAML param resolving to all three of `IcebergSparkSessionExtensions` + `SparkSessionCatalog` + `spark_catalog.type=hive`. Two of three is broken.
6. **MoR justification is real** — for every MoR table, `decisions.yml` names a *pre-existing* mutation as the reason (not a skill-introduced MERGE), and its MERGE `ON` key is unique in the target (includes the version column for SCD). If not, the mode/key is wrong — fix the DDL/DML, not just the note.
7. **New wf is internally consistent** — the maintenance wf's name is project-appropriate (not copied verbatim from a sibling, e.g. no `-al-` in a `fc` project — `grep -rn '<sibling_prefix>' mart.yml ctl.yml` must be empty), its YAML anchors (`*wfDefaultInfo`, `*wfParamsDefault`, `oozie.wf.application.path`) exist in *this* project, and any referenced oozie app (`hdfs_care`) is actually present.
8. **MoR maintenance covers every MoR table, not a subset** — for each table whose `decisions.yml` mode is MoR: its `rewrite_data_files` call has `delete-file-threshold`, and `rewrite_position_delete_files` + `rewrite_manifests` are called **for it specifically** (`grep -n '<table>' upd_iceberg_*.sql` per procedure). A shared maintenance script that applies the MoR-aware procedures to 2 of 5 MoR tables leaves the other 3 silently degrading — extend the script, don't average it out.
9. **entity_id provenance** — every `entity.id` in wf entries you added appears elsewhere in the project (`grep -rn '<id>' src/main/resources/` beyond your own edit). An ID with no other occurrence is invented — replace it with the real one from `ctl.yml`/`1_ctl_entities.yml` or ask the user.

Append each pass to `verify_log`, then re-run. Only report completion when the last pass has an empty failure list — and even then, list what was left as Parquet by design so it reads as a decision, not an omission.

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

⚠ **Do NOT convert an append-only / SCD-versioned / full-reload load into a `MERGE INTO` keyed on the business key.** This is the single worst outcome of a mechanical rewrite:

- On an **SCD/versioned** target (rows are versions; partition `part_ctl_validfrom`/`ctl_loading`; columns `ctl_validfrom`/`ctl_validto`), a `MERGE ... ON <business_key>` matches **every historical version** of that key. `WHEN MATCHED THEN UPDATE SET *` then overwrites all past versions with today's values — **silent history corruption, no error raised.** If the source is itself versioned, Spark instead aborts with a cardinality violation (*"a single target row matched multiple source rows"*).
- The MERGE `ON` key must be **unique in the target** (include the version column, e.g. `..., ctl_validfrom`) — otherwise the statement is wrong regardless of mode. A version-insert load is an **`INSERT`/append**, not a MERGE.
- Under **MoR**, every one of those spurious updates also emits position/equality delete files → delete explosion on top of the corruption.

So: only propose the MERGE rewrite for tables the interview classified as **true upsert targets** with a confirmed unique key. For append/SCD/full-reload tables, leave the load as append/overwrite (and keep them CoW or Parquet). This is why the mode decision depends on the interview, not on whether a MERGE appears in the DML.

Full pattern catalogue with before/after SQL examples: [reference.md § Iceberg-native pipeline optimizations](./reference.md#iceberg-native-pipeline-optimizations).

Confirm in your announce that you will run this pipeline analysis pass and surface findings to the user.

### ⚠ MANDATORY — if the target table already says `USING iceberg`, audit the full TBLPROPERTIES block, don't just patch the property the user named

A common way this skill gets invoked is not "convert Parquet to Iceberg" but "this table is already on Iceberg, now do X" — switch CoW→MoR, fix retention, add a maintenance wf for a domain migrated in an earlier pass. **Detect this case before touching any DDL**:

```bash
grep -n 'USING iceberg' <project>/src/main/resources/sql/ddl/<target-domain>/*.sql
```

If it's already there, an earlier pass (this skill or a human) already migrated the table — and that pass may itself have used an incomplete property set, especially if it predates this skill's current "Property reference" table (Step 6 below). **Do not limit your edit to the specific property the user asked about.** Read the table's full existing `TBLPROPERTIES` block and diff it, property by property, against every row of the "Property reference" table in Step 6. Any "Always" or "Default" property missing from the existing DDL is tech debt from the prior pass — surface it in the Phase A plan and fix it in the same edit, even though the user's literal request only named one property.

**Concretely:** if the user asks "switch this table to MoR" and the existing block is

```sql
TBLPROPERTIES (
  'format-version'='2',
  'write.parquet.compression-codec'='zstd',
  'write.update.mode'='copy-on-write',
  'write.delete.mode'='copy-on-write',
  'write.merge.mode'='copy-on-write')
```

— that's 5 of the 11 properties the Property reference table calls for. Flipping the three `write.*.mode` values to `merge-on-read` and stopping there is **not** a complete edit, it just re-commits the original migration's gap with a different mode value. In the same pass, add the missing `write.format.default`, `write.target-file-size-bytes`, `write.distribution-mode`, `write.metadata.delete-after-commit.enabled`, `write.metadata.previous-versions-max`, and `comment` — using the same per-property sourcing rules as Step 6 (sibling-project lookup for the codec, the table's own compaction script for `target-file-size-bytes`, never hardcode `write.bloom.filter.columns`).

State the finding explicitly in Phase A, one line per affected table: `"<table>: existing TBLPROPERTIES has N/11 properties from the reference table — bringing to full set while applying the requested mode change."` If the user explicitly says "only change the mode, don't touch anything else," respect that — but still name what you're leaving incomplete and why, so it's a visible decision rather than a silent gap that resurfaces next time someone touches this table.

This applies to **every** table the task's scope covers, not just the first one you happen to open — a domain-level request ("migrate `custom_blago_fc/bond` to MoR") means every table in that domain gets the same audit, not just whichever table you read first.

### ⚠ MANDATORY — per-table clarifying interview (BLOCK before choosing a mode or rewriting any DML)

Reconnaissance tells you what the code *looks like*; it does not tell you what the table *is*. The two most damaging failure modes of this skill — flipping an append/SCD table to MoR, and rewriting a version-insert load into a `MERGE` that corrupts history — both come from **guessing the write pattern instead of asking**. So after recon and before the plan, run a short interview. **First check `./.iceberg-migration/decisions.yml`: for any table already answered there with no upstream change, reuse the saved answer and skip the question** (say which). Ask only what is genuinely unknown; batch the questions into one message. Do not present the plan or touch DDL/DML until every in-scope table has answers.

Per table in scope, you need:

**Format — infer first, confirm second (respect the user's time).** Do not fire 6 raw questions × N tables. Reconnaissance already gives you a defensible guess for most answers — fill the table below yourself and present it as **one message** for the user to confirm or correct. Inference sources: layer from the schema suffix (`*_aux` → AUX, `*_ini` → INI, main schema → AL/PA) + the ICEBERG_WF_GUIDE data-flow; write pattern from the *current* DML verbs plus the SCD tells (`ctl_validfrom`/`ctl_validto` columns, partition on `part_ctl_validfrom`/`ctl_loading`); unique key from S2T constraints or an existing MERGE `ON` clause (mark it "unverified" if that's the only source). A wrong guess costs the user one correction; six open questions per table cost the interview itself.

```
### Per-table interview (confirm or correct each row — the plan is blocked on this)
| Table | Layer | Write pattern | Unique key (target row) | Pre-existing row-level UPD/DEL? | Proposed | Basis |
|---|---|---|---|---|---|---|
| aux.t_bond | AUX | scd_versioned (ctl_validfrom/validto, part_ctl_validfrom) | (business_key…, ctl_validfrom) | none | keep parquet (AUX) | guide layer rule |
| pa.t_agr_bond | AL | upsert | (epk_id) — from MERGE ON, unverified | yes — t_agr_bond.sql:442 | Iceberg MoR | pre-existing MERGE |
```

The six answers each row must resolve (1–4 are the table's columns; **5–6 are not columns** — ask them as a short list under the table: 5 as a per-table Y/N only for rows where a MERGE rewrite is actually proposed, 6 once globally unless a specific table needs different values):

1. **Layer** — AUX / HIST / AL (or project equivalent)? Per the ICEBERG_WF_GUIDE data-flow (`Sources → AUX (Parquet) → HIST (Iceberg) → AL (Iceberg)`), **AUX defaults to staying Parquet.** If an AUX table is in scope, confirm explicitly whether it should be migrated at all — do not migrate it silently.
2. **Write pattern** — which one: `append-only` / `INSERT OVERWRITE` (full reload) / `true upsert` (row-level update of current rows) / `SCD-versioned` (each load inserts a new version, old versions retained)? This — not the presence of a MERGE — is what decides MoR vs CoW vs leave-as-Parquet. Watch for the SCD/append tells: `ctl_validfrom`/`ctl_validto`/`ctl_pa_loading` columns, partition on `part_ctl_validfrom`/`ctl_loading`.
3. **Real unique key** — the key that is unique *in the target after this load*. For SCD/versioned tables this MUST include the version column (e.g. `ctl_validfrom`), not just the business key — a business-key-only MERGE matches every historical version. If the user isn't sure, ask; do not infer the key from a `GROUP BY` or an existing `ON` clause.
4. **Genuine row-level deletes?** — does anything today delete/update *current* rows (not "insert a tombstone", not "overwrite the partition")? Only a "yes" justifies MoR. If deletes are actually tombstone inserts or full-partition overwrites, it's CoW/append.
5. **Permission to rewrite `SELECT`/`INSERT` → `MERGE INTO`** — the pipeline-analysis pass may *propose* this, but you may not apply it without an explicit yes for that table. A "no" keeps the existing load semantics (and usually means CoW/append, not MoR).
6. **Non-defaultable property values** — `write.parquet.compression-codec`, `write.target-file-size-bytes`, `write.bloom.filter.columns`, `write.sort-order`, `write.distribution-mode`: confirm the source (sibling project / this table's own compaction script) rather than copying another table's values. Never hardcode the same `bloom.filter.columns` / `sort-order` across tables with different keys.

Record every answer to `./.iceberg-migration/decisions.yml` immediately (see "Persistent decisions & re-runs"). In the Phase A plan, for **each** MoR table, print the interview answer that justifies MoR — and it must be a *pre-existing* mutation, never "the MERGE this skill added." If you cannot name one, the table is not MoR.

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
  'format-version'                            = '2',
  'write.format.default'                      = 'parquet',
  'write.parquet.compression-codec'           = '<codec>',         -- see table below — do not default blindly
  'write.target-file-size-bytes'              = '<bytes>',         -- see table below — do not hardcode a project-wide constant
  'write.distribution-mode'                   = 'none',
  'write.update.mode'                          = 'merge-on-read',  -- or 'copy-on-write' — all three modes below MUST agree, see "Mode selection" above
  'write.delete.mode'                          = 'merge-on-read',
  'write.merge.mode'                           = 'merge-on-read',
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max'       = '10',
  'comment'                                    = '<from S2T Tables sheet Description column, verbatim — never invent>'
  -- 'write.bloom.filter.columns'              = '<col>',          -- OPTIONAL, never hardcode — see table below
);
```

**Use the full block above as the default for every migrated table — do not strip it down to just `format-version` + the three `write.*.mode` lines.** Those four are the only ones that are *always* mandatory, but that does not make the other six optional: `write.format.default`, `write.parquet.compression-codec`, `write.target-file-size-bytes`, `write.distribution-mode`, `write.metadata.delete-after-commit.enabled`, and `write.metadata.previous-versions-max` are **defaults you include on every table unless you have a specific, stated reason to omit one** — "I was in a hurry" is not such a reason. A DDL that only has `format-version` + the three modes is a sign you under-applied this section, not a valid minimal migration. Only `write.bloom.filter.columns` is genuinely conditional — omit it when no column qualifies (see its row below). `comment` should be included for every table too; its only caveat is sourcing (pull from S2T, never invent), not whether to include it at all.

**Property reference — what each line means and how to decide its value.** Skipping the reasoning column and pasting a fixed value across every table (e.g. copying `'gzip'` or `'ctl_validfrom'` from one table's example into another) is exactly the mistake this section exists to prevent.

| Property | Include by default? | How to determine the value |
|---|---|---|
| `format-version` | Always | `'2'` |
| `write.update.mode` / `write.delete.mode` / `write.merge.mode` | Always, all three together | MoR or CoW per "Mode selection" rule above — never mix modes across the three. |
| `write.format.default` | Default — omit only with a stated reason | `'parquet'` — stable across the project, safe to always include. |
| `write.parquet.compression-codec` | Default — omit only with a stated reason | **Check the Phase A.7 sibling project's Iceberg DDL first** (`grep -rn "compression-codec" <sibling>/sql/ddl/`) and reuse whatever codec it already runs in production — that's a real, validated choice. Only fall back to a skill default (`'zstd'`) when no sibling exists. Do not assume the old Hive `PARQUET.COMPRESS` value transfers (e.g. `SNAPPY` pre-migration does not imply `snappy` post-migration) and do not copy a codec from an unrelated example table. |
| `write.target-file-size-bytes` | Default — omit only with a stated reason | **Do not hardcode one number for the whole project.** First check the sibling project's convention. If none, reuse the `target-file-size-bytes` value already chosen for *this table's* `upd_iceberg_<table>.sql` compaction script (ICEBERG_WF_GUIDE templates default to `134217728` = 128 MiB) so write-time and compaction-time targets agree — picking different numbers for the two means compaction immediately starts rewriting freshly-written files. Scale up (256–512 MiB) for large append-heavy fact tables, down (64 MiB) for small slowly-growing dimension tables, based on existing parquet file sizes for that table (`hdfs dfs -du` or existing partition stats) — state the chosen size and why in the plan, don't silently pick a default. |
| `write.distribution-mode` | Default — omit only with a stated reason | `'none'` is the safe default when upstream already writes one file per partition reasonably. Switch to `'hash'` only if you observe many small files per partition in the existing parquet layout — confirm with the user first, since it changes write-time shuffle cost. |
| `write.metadata.delete-after-commit.enabled` | Default — omit only with a stated reason | `'true'` — without it `metadata.json` files accumulate forever; safe to always set. |
| `write.metadata.previous-versions-max` | Default — omit only with a stated reason | `'10'` — keep this numerically aligned with `app.sql.retain_snapshots` in the table's compaction wf (ICEBERG_WF_GUIDE default is also `10`); if you change one, change the other. |
| `write.bloom.filter.columns` | **Never default — set only when justified per table** | Set this **only** when the table has an obvious high-cardinality equality-lookup column: the join key used in a `MERGE INTO ... ON` (Pattern 1 rewrite) or the `sort_order` column already chosen for *this table's* `upd_iceberg_<table>.sql`. Derive it from that table's own DML/sort_order — never copy a column name from a different table's example (e.g. `ctl_validfrom` is only relevant if this table's compaction script actually sorts/filters by it). If no single column stands out as the dominant filter/join key, omit the property entirely rather than guessing. |
| `comment` | Default — omit only with a stated reason | Pull verbatim from the `Description` column of the S2T `Tables` sheet for this table. Never invent one. |

**Post-write sanity check:** after writing each table's DDL, count the `write.*`/`format-version`/`comment` keys actually present in the `TBLPROPERTIES` block. Fewer than 11 (`format-version` + the 3 `write.*.mode` keys + the other 7 "Default" rows including `comment` — i.e. everything except the one "Never default" row, `write.bloom.filter.columns`) means you likely fell back to the bare minimum — go back and fill in the missing defaults, or write down the specific reason you're omitting each one.

2. Wire the table into compaction per [ICEBERG_WF_GUIDE.md](./ICEBERG_WF_GUIDE.md) — this is **not optional**:

   - Create `src/main/resources/sql/dml/exp_iceberg_<table>.sql` (expire_snapshots) — template in ICEBERG_WF_GUIDE "Expire snapshots only".
   - Create `src/main/resources/sql/dml/upd_iceberg_<table>.sql` (full cycle) — **template choice depends on mode**:
     - **MoR table** (`write.delete.mode=merge-on-read`): use the MoR-aware template — "Полный цикл обслуживания для MoR-таблиц" in ICEBERG_WF_GUIDE. It adds `delete-file-threshold` to `rewrite_data_files`, plus `rewrite_position_delete_files` and `rewrite_manifests` calls. **Required for MoR** — without it, position-delete files accumulate, reads degrade silently.
     - **CoW table** (default): use "Полный цикл обслуживания" — basic `rewrite_data_files` + `remove_orphan_files`.
   - Using the CoW template on a MoR table is a silent footgun. The Phase B plan table records the chosen mode per table; cross-check it here.
   - **Creating these two `.sql` files is not the end of this step — they are inert until wired into a wf.** A run that writes `exp_iceberg_<table>.sql` / `upd_iceberg_<table>.sql` and stops there has not actually scheduled compaction; nothing will ever call them. Continue with the two sub-steps below in the same pass, before moving to the Gherkin step.
     - Add the two `spark_driver_extraJavaOptions__hdfs_care_*` params for the table to the **project's existing maintenance wf** in `src/main/resources/wf/ctl/`. Find it first: `grep -rn 'rewrite_data_files\|expire_snapshots\|exp_iceberg\|hdfs_care' src/main/resources/wf/ctl/`. Common names that may already exist: `wf_schema_hdfs_care`, `wf_<table>_service`, or a project-specific variant. If nothing found, follow ICEBERG_WF_GUIDE "Сценарий 2: Создай отдельный wf для таблицы" and pick a name matching the project's convention — propose 2–3 candidates to the user (`wf_<table>_service` for per-table, `wf_schema_hdfs_care` / `wf_iceberg_maintenance` for shared) and confirm before creating. Actually edit `ctl.yml` (or `ctl_<category>.yml`) — do not just describe the params in the chat.
     - Check that the `${app.sql.*}` placeholders referenced inside the two scripts you just wrote (`app.sql.safe_days`, `app.sql.retain_snapshots`, `app.sql.service_date_from`) and the `ssc_schema_hdfs_care` (or project-specific) Spark conf param are actually defined somewhere reachable by that wf — `grep -rn 'app.sql.safe_days\|app.sql.retain_snapshots\|app.sql.service_date_from\|date_achive_from' src/main/resources/wf/`. If missing, add them to `mart.yml`/`ctl.yml` next to where the sibling project defines them. Without this, the compaction wf runs and fails immediately on an undefined parameter — a broken migration that looks complete because the `.sql` files exist.
   - Use `entity_id` captured in Step 3.
   - **Self-check before moving on** — do not consider this table's compaction wiring done until all four are true: (1) `exp_iceberg_<table>.sql` exists, (2) `upd_iceberg_<table>.sql` exists, (3) `git diff`/your edit history shows an actual change to `ctl.yml` referencing both scripts by path, (4) the `app.sql.*` / `ssc_schema_hdfs_care` params they depend on resolve to a definition somewhere in `mart.yml`/`ctl.yml`. If (3) or (4) didn't happen, go back — this is the single most common incomplete-migration failure mode for this skill.
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
