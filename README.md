# iceberg-migration-skill

A Claude Code skill that walks the agent through migrating a project from Apache Parquet / Hive-parquet (or ORC) to Apache Iceberg. Python, Java, and Scala projects supported.

Markdown-only — no runtime dependencies, no install step. The skill drives the agent's built-in tools (`Read`, `Edit`, `Bash` with `grep` / `find` / `rg`).

## Install (user-level skill)

```bash
git clone <this-repo> ~/git/iceberg-migration-skill
ln -s ~/git/iceberg-migration-skill ~/.claude/skills/iceberg-migration-skill
```

Claude Code picks up the skill on the next session start. Invoke it by asking for an Iceberg migration ("convert parquet", "migrate to iceberg", "migrate hive to iceberg" — the full list of triggers is in `SKILL.md`'s frontmatter `description`).

## What's in the box

- `SKILL.md` — the workflow: recon → migration plan → apply. Drives the whole conversation.
- `S2T_GUIDE.md` — Source-to-Target spec system for Hadoop/Spark datamart projects.
- `ICEBERG_WF_GUIDE.md` — Oozie-based `wf/ctl/*.yml` workflow conventions, Spark conf, compaction templates.
- `examples.md` — before/after rewrites for the common patterns.
- `reference.md` — operational concerns, phased migration runbook, MoR/CoW, edge cases.

## Origin

Forked from the `open-table-migrator` skill at commit `4622947`. That version ships a Python CLI module that performs static analysis; this fork drops the Python module and leans on the agent's built-in search tools instead.
