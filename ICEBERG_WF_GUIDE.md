# 📘 Инструкция по работе с Iceberg в wf/ctl

## Содержание
1. [Общая архитектура](#общая-архитектура)
2. [Параметры для Iceberg в wf/ctl](#параметры-для-iceberg)
3. [Структура ctl-файлов](#структура-ctl-файлов)
4. [Поиск готовых wf для compaction](#поиск-готовых-wf)
5. [Как создать новый wf для compaction](#как-создать-новый-wf)

---

## Общая архитектура

### Слои проекта
```
src/main/resources/
├── wf/
│   ├── ctl/          # Определения wf в YAML (напр. ctl.yml, ctl_ecod.yml)
│   └── oozie/        # Oozie workflow XML
├── sql/
│   ├── ddl/          # DDL таблиц (CREATE TABLE)
│   ├── dml/          # DML скрипты (INSERT, UPDATE, CALL)
│   └── service/      # Service-скрипты (WMS-*.sql)
```

### Поток данных
```
Sources → AUX (Parquet) → HIST (Iceberg) → AL (Iceberg)
                ↓              ↓                ↓
         INSERT INTO      COMPACT + EXPIRE   COMPACT + EXPIRE
```

---

## Параметры для Iceberg

### Обязательные параметры Spark для Iceberg

```yaml
spark_driver_extraJavaOptions: >
  -Dapp.ctl.loggerShort=true
  -Dapp.hdfs.file.path={{devops.datamart_path_app}}/sql/dml/<script>.sql
  -Dapp.publish.entity.id=<ENTITY_ID>

# Для compaction/expire:
-Dapp.sql.service_date_from={{mart.date_achive_from}}
-Dapp.sql.safe_days=2
-Dapp.sql.retain_snapshots=10
```

### Spark-конфигурация для Iceberg

```yaml
spark_submit_cmd_iceberg_service: >
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
  --conf spark.sql.catalog.spark_catalog.type=hive
```

### Переменные окружения (из `{{mart.*}}`)

| Переменная | Описание |
|------------|----------|
| `{{mart.date_achive_from}}` | Дата архива (например, `20250101`) |
| `{{mart.hdfs_path_app_full}}` | Путь к приложению в HDFS |
| `{{devops.datamart_path_app}}` | Путь к datamart в HDFS |
| `{{hdfs_path_datamart}}` | Путь к warehouse Iceberg |

---

## Структура ctl-файлов

### Базовая структура workflow

```yaml
- wf_<название>:
    <<: *wfDefaultInfo          # Наследование параметров
    <<: *wfDefaultNotifications # Уведомления
    category: "{{mart.ctl_category_<категория>>"
    name: "{{wf_<название>>"

    params:
      - *wfDefaultParams
      - *wfDefaultKrbParams
      - param:
          name: oozie.wf.application.path
          prior_value: "{{devops.datamart_path_app}}/<path>"
      - param:
          name: spark_submit_class_path
          prior_value: "--class <your-org>.<package>.wf.<Class> {{hdfs_path_app_jar}}"
      - param:
          name: spark_driver_extraJavaOptions
          prior_value: >
            "{{mart.spark_driver_extraJavaOptions}}
             -Dapp.<param1>=<value1>
             -Dapp.<param2>=<value2>"
      - param:
          name: spark_submit_cmd
          prior_value: "{{mart.spark_submit_cmd}} {{mart.ssc_<название>>"

    schedule_params:
      <<: *scheduleParams3
      eventAwaitStrategy: "and"
      entities:
        - entity: { active: true, statisticId: 2, id: <ENTITY_ID>, profile: "{{global.CTL_PROFILE_NAME}}" }

    schedule:
      type: "{{mart.schedule_type}}"
```

### Параметры lock-operations

```yaml
init_locks:
  checks:
    ## TARGET (на запись)
    - check: { entity_id: <ENTITY_ID>, <<: *ctlCheckLockWrite }
    ## SOURCES (на чтение)
    - check: { entity_id: <SOURCE_ENTITY_ID>, <<: *ctlCheckLockRead }

  sets:
    ## TARGET (на чтение и запись)
    - set: { entity_id: <ENTITY_ID>, <<: *ctlSetLockRead }
    - set: { entity_id: <ENTITY_ID>, <<: *ctlSetLockWrite }
    ## SOURCES (на запись)
    - set: { entity_id: <SOURCE_ENTITY_ID>, <<: *ctlSetLockWrite }
```

### Параметры для compaction (hdfs_care)

```yaml
- wf_<table>_service:
    params:
      - param:
          name: oozie.wf.application.path
          prior_value: "{{devops.datamart_path_app}}/hdfs_care"

      # Expire snapshots
      - param:
          name: spark_driver_extraJavaOptions__hdfs_care_exp_snp_<table_name>
          prior_value: >
            "{{mart.spark_driver_extraJavaOptions}}
             -Dapp.ctl.loggerShort=true
             -Dapp.hdfs.file.path={{devops.datamart_path_app}}/sql/dml/exp_iceberg_<table_name>.sql
             -Dapp.publish.entity.id=<ENTITY_ID>"

      # Rewrite data files
      - param:
          name: spark_driver_extraJavaOptions__hdfs_care_<table_name>
          prior_value: >
            "{{mart.spark_driver_extraJavaOptions}}
             -Dapp.ctl.loggerShort=true
             -Dapp.hdfs.file.path={{devops.datamart_path_app}}/sql/dml/upd_iceberg_<table_name>.sql
             -Dapp.publish.entity.id=<ENTITY_ID>"

      # Параметры для скриптов
      - param:
          name: app.sql.service_date_from
          prior_value: "{{mart.date_achive_from}}"
      - param:
          name: app.sql.safe_days
          prior_value: "2"
      - param:
          name: app.sql.retain_snapshots
          prior_value: "10"

      - param:
          name: spark_submit_cmd
          prior_value: "{{mart.spark_submit_cmd}} {{mart.ssc_schema_hdfs_care}}"
```

---

## Поиск готовых wf для compaction

### Шаг 1: Проверь `ctl.yml` — готовый wf `wf_schema_hdfs_care`

```bash
grep -A 50 "wf_schema_hdfs_care" src/main/resources/wf/ctl/ctl.yml
```

В этом wf уже настроены скрипты для:
- `t_pfm_agr_bal`
- `t_pfm_agr_bal_json`
- `t_pfm_ecod_account_daily`
- `t_pfm_ecod_dep_daily`
- `t_pfm_ecod_dep`
- `t_pfm_group_indicator_daily`
- `t_pfm_group_indicator`

### Шаг 2: Ищи по имени таблицы

```bash
# В ctl-файлах
grep -rn "t_agr_news\|t_pfm_ecod_dep" src/main/resources/wf/ctl/

# В скриптах DML
grep -rn "t_agr_news" src/main/resources/sql/dml/
```

### Шаг 3: Проверь существование скриптов

```bash
ls -la src/main/resources/sql/dml/ | grep -E "exp_iceberg|upd_iceberg"
```

Скрипты должны называться:
- `exp_iceberg_<table_name>.sql` — для `expire_snapshots`
- `upd_iceberg_<table_name>.sql` — для `rewrite_data_files` + `remove_orphan_files`

---

## Как создать новый wf для compaction

### Сценарий 1: Уже есть wf `wf_schema_hdfs_care`, но нет таблицы в нем

**Решение:** Добавь параметры для новой таблицы в `wf_schema_hdfs_care`

```yaml
# В wf_schema_hdfs_care добавь:
- param:
    name: spark_driver_extraJavaOptions__hdfs_care_exp_snp_t_<new_table>
    prior_value: >
      "{{mart.spark_driver_extraJavaOptions}}
       -Dapp.ctl.loggerShort=true
       -Dapp.hdfs.file.path={{devops.datamart_path_app}}/sql/dml/exp_iceberg_t_<new_table>.sql
       -Dapp.publish.entity.id=<ENTITY_ID>"

- param:
    name: spark_driver_extraJavaOptions__hdfs_care_t_<new_table>
    prior_value: >
      "{{mart.spark_driver_extraJavaOptions}}
       -Dapp.ctl.loggerShort=true
       -Dapp.hdfs.file.path={{devops.datamart_path_app}}/sql/dml/upd_iceberg_t_<new_table>.sql
       -Dapp.publish.entity.id=<ENTITY_ID>"
```

### Сценарий 2: Создай отдельный wf для таблицы

**Шаг 1:** Создай скрипты DML

```sql
-- exp_iceberg_<table_name>.sql
set safe_date = (select cast(date_sub(current_date,${app.sql.safe_days}) as timestamp));
CALL spark_catalog.system.expire_snapshots(
    table => '<schema>.<table_name>',
    retain_last => ${app.sql.retain_snapshots},
    older_than => timestamp'$safe_date');

-- upd_iceberg_<table_name>.sql
set USE_CUSTOM_UPDATE=false;
set safe_date = (select cast(date_sub(current_date,${app.sql.safe_days}) as timestamp));

CALL spark_catalog.system.expire_snapshots(
    table => '<schema>.<table_name>',
    retain_last => ${app.sql.retain_snapshots},
    older_than => timestamp'$safe_date');

CALL spark_catalog.system.rewrite_data_files(
  table => '<schema>.<table_name>',
  where => '<partition_column> >= ${app.sql.service_date_from}',  -- substitute THIS table's real partition column (from its PARTITIONED BY, same as sort_order below); carrying the template's part_report_dt onto a table partitioned otherwise makes rewrite error or silently no-op
  strategy => 'sort',
  sort_order => '<partition_column> asc nulls last',
  options => map(
    'target-file-size-bytes', '134217728',
    'partial-progress.enabled','true'));

CALL spark_catalog.system.remove_orphan_files(
  table => '<schema>.<table_name>',
  older_than => timestamp '$safe_date');
```

**Шаг 2:** Добавь wf в соответствующий `ctl_<category>.yml`

```yaml
- wf_<table_name>_service:
    <<: *wfDefaultInfo
    <<: *wfDefaultNotifications
    category: "{{mart.ctl_category_<category>>"
    name: "{{wf_<table_name>_service>>"
    singleLoading: false

    params:
      - *wfDefaultParams
      - *wfDefaultKrbParams
      - param:
          name: oozie.wf.application.path
          prior_value: "{{devops.datamart_path_app}}/hdfs_care"
      - param:
          name: description
          prior_value: "{{wf_<table_name>_service>>"
      - param:
          name: spark_submit_class_path
          prior_value: "--class <your-org>.<package>.wf.RunSqlSP {{hdfs_path_app_jar}}"
      - param:
          name: spark_driver_extraJavaOptions__hdfs_care_exp_snp_<table_name>
          prior_value: >
            "{{mart.spark_driver_extraJavaOptions}}
             -Dapp.ctl.loggerShort=true
             -Dapp.hdfs.file.path={{devops.datamart_path_app}}/sql/dml/exp_iceberg_<table_name>.sql
             -Dapp.publish.entity.id=<ENTITY_ID>"
      - param:
          name: spark_driver_extraJavaOptions__hdfs_care_<table_name>
          prior_value: >
            "{{mart.spark_driver_extraJavaOptions}}
             -Dapp.ctl.loggerShort=true
             -Dapp.hdfs.file.path={{devops.datamart_path_app}}/sql/dml/upd_iceberg_<table_name>.sql
             -Dapp.publish.entity.id=<ENTITY_ID>"
      - param:
          name: app.sql.service_date_from
          prior_value: "{{mart.date_achive_from}}"
      - param:
          name: app.sql.safe_days
          prior_value: "2"
      - param:
          name: app.sql.retain_snapshots
          prior_value: "10"
      - param:
          name: spark_submit_cmd
          prior_value: "{{mart.spark_submit_cmd}} {{mart.ssc_schema_hdfs_care}}"
      - param:
          name: spark_submit_cmd_exp_snp
          prior_value: "{{mart.spark_submit_cmd}} {{mart.ssc_schema_hdfs_care}} --conf spark.sql.autoBroadcastJoinThreshold=-1"

    schedule_params:
      <<: *scheduleParams3
      eventAwaitStrategy: "and"
      cron: { active: true, expression: "0 22 * * 0" }  # Каждое воскресенье в 22:00

    schedule:
      type: "{{mart.schedule_type}}"
```

**Шаг 3:** Получи ENTITY_ID из `ctl_entities.yml`

```bash
grep -A 5 "t_agr_news" src/main/resources/wf/ctl/1_ctl_entities.yml
# Entity ID: 920031144
```

---

## Шаблоны скриптов

### Expire snapshots only
```sql
set safe_date = (select cast(date_sub(current_date,${app.sql.safe_days}) as timestamp));

CALL spark_catalog.system.expire_snapshots(
    table => '<schema>.<table_name>',
    retain_last => ${app.sql.retain_snapshots},
    older_than => timestamp'$safe_date');
```

### Полный цикл обслуживания (для CoW-таблиц)
Используй этот шаблон для таблиц **без** row-level UPDATE / DELETE / MERGE (Copy-on-Write по умолчанию).

```sql
set USE_CUSTOM_UPDATE=false;
set safe_date = (select cast(date_sub(current_date,${app.sql.safe_days}) as timestamp));

-- Expire snapshots
CALL spark_catalog.system.expire_snapshots(
    table => '<schema>.<table_name>',
    retain_last => ${app.sql.retain_snapshots},
    older_than => timestamp'$safe_date');

-- Rewrite data files
CALL spark_catalog.system.rewrite_data_files(
  table => '<schema>.<table_name>',
  where => '<partition_column> >= ${app.sql.service_date_from}',  -- substitute THIS table's real partition column (from its PARTITIONED BY, same as sort_order below); carrying the template's part_report_dt onto a table partitioned otherwise makes rewrite error or silently no-op
  strategy => 'sort',
  sort_order => '<partition_column> asc nulls last',
  options => map(
    'target-file-size-bytes', '134217728',
    'partial-progress.enabled','true'));

-- Remove orphan files
CALL spark_catalog.system.remove_orphan_files(
  table => '<schema>.<table_name>',
  older_than => timestamp '$safe_date');
```

### Полный цикл обслуживания для MoR-таблиц

**Обязательный шаблон** для таблиц с `write.delete.mode=merge-on-read` / `write.update.mode=merge-on-read` / `write.merge.mode=merge-on-read`. Без `delete-file-threshold` + `rewrite_position_delete_files` data-файлы с position-deletes не переписываются, scan'ы деградируют — это тихий регресс.

Требования: Iceberg ≥ 1.4 для `rewrite_position_delete_files` + `rewrite_manifests`.

```sql
set USE_CUSTOM_UPDATE=false;
set safe_date = (select cast(date_sub(current_date,${app.sql.safe_days}) as timestamp));

-- Expire snapshots
CALL spark_catalog.system.expire_snapshots(
    table => '<schema>.<table_name>',
    retain_last => ${app.sql.retain_snapshots},
    older_than => timestamp'$safe_date');

-- Rewrite data files with MoR-aware option: rewrite files that have >= 2 associated deletes
CALL spark_catalog.system.rewrite_data_files(
  table => '<schema>.<table_name>',
  where => '<partition_column> >= ${app.sql.service_date_from}',  -- substitute THIS table's real partition column (from its PARTITIONED BY, same as sort_order below); carrying the template's part_report_dt onto a table partitioned otherwise makes rewrite error or silently no-op
  strategy => 'sort',
  sort_order => '<partition_column> asc nulls last',
  options => map(
    'target-file-size-bytes', '134217728',
    'partial-progress.enabled', 'true',
    'delete-file-threshold', '2'));

-- Compact the position-delete files themselves
CALL spark_catalog.system.rewrite_position_delete_files(
  table => '<schema>.<table_name>',
  options => map('rewrite-all', 'true'));

-- Compact manifest list (speeds up scan planning)
CALL spark_catalog.system.rewrite_manifests('<schema>.<table_name>');

-- Remove orphan files
CALL spark_catalog.system.remove_orphan_files(
  table => '<schema>.<table_name>',
  older_than => timestamp '$safe_date');
```

**Параметры для `delete-file-threshold`:** `2` — компромисс между частотой переписывания и накоплением deletes. На горячих таблицах с большим количеством UPDATE/DELETE — `1`. На редко-меняющихся MoR — `5`.

---

## Частота запуска compaction

| Таблица | Cron | Пояснение |
|---------|------|-----------|
| High-volume | `0 22 * * 0` | Еженедельно |
| Medium-volume | `0 22 * * 0` | Еженедельно |
| Low-volume | `0 22 * * 0` | Еженедельно |

**Параметры:**
- `app.sql.safe_days = 2` — хранить минимум 2 дня
- `app.sql.retain_snapshots = 10` — хранить минимум 10 snapshot'ов

---

## Типичные ошибки и решения

| Проблема | Решение |
|----------|---------|
| `Entity not found` | Проверь `1_ctl_entities.yml` для правильного `entity_id` |
| `Catalog not found` | Добавь `spark_catalog` конфигурацию |
| `Write conflict` | Установи `rewrite.partial-progress.enabled=true` |
| `Lock violation` | Добавь правильные `init_locks` в wf |

---

## Контрольный список перед созданием wf

- [ ] Создан `exp_iceberg_<table_name>.sql`
- [ ] Создан `upd_iceberg_<table_name>.sql`
- [ ] Получен `entity_id` из `1_ctl_entities.yml`
- [ ] Определена `category` (из `categories:` в `ctl.yml`)
- [ ] Проверен `wf_schema_hdfs_care` — может быть уже есть
- [ ] Добавлен wf в `ctl_<category>.yml`
- [ ] Проверено cron-выражение (например, `0 22 * * 0`)
- [ ] Проверен `spark_submit_cmd` — используется `ssc_schema_hdfs_care`

---

## Полезные команды

```bash
# Найти entity_id
grep -A 5 "<table_name>" src/main/resources/wf/ctl/1_ctl_entities.yml

# Найти существующие compaction wf
grep -rn "hdfs_care" src/main/resources/wf/ctl/

# Проверить структуру таблицы
grep -A 30 "CREATE TABLE.*<table_name>" src/main/resources/sql/ddl/<layer>/

# Найти DML для таблицы
ls -la src/main/resources/sql/dml/ | grep -i <table_name>
```

## DDL: full TBLPROPERTIES template and per-property sourcing rules (canonical)

This is the canonical DDL property block the skill points at (its DDL step and its TBLPROPERTIES-completeness probe). Every migrated table's DDL uses the full block (11 properties); the reference table under it explains how to source each value — pasting one fixed value across all tables is the mistake it prevents.

Generate / regenerate the DDL from `s2t.xlsx` per S2T_GUIDE "Контрольный список перед запуском" (the DDL generator step that turns S2T into `src/main/resources/sql/ddl/<layer>/<table>.sql`), then change `STORED AS PARQUET` → `USING iceberg`:

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
  'write.update.mode'                          = 'merge-on-read',  -- or 'copy-on-write' — all three mode keys MUST agree (never mixed)
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
| `write.update.mode` / `write.delete.mode` / `write.merge.mode` | Always, all three together | MoR or CoW — all three set together to the same mode, never mixed. MoR only for tables with pre-existing row-level UPDATE/DELETE/MERGE in the pipeline; append/SCD-versioned/overwrite/log tables are CoW (or stay Parquet). |
| `write.format.default` | Default — omit only with a stated reason | `'parquet'` — stable across the project, safe to always include. |
| `write.parquet.compression-codec` | Default — omit only with a stated reason | **Check the sibling project's Iceberg DDL first** (`grep -rn "compression-codec" <sibling>/sql/ddl/`) and reuse whatever codec it already runs in production — that's a real, validated choice. Only fall back to a skill default (`'zstd'`) when no sibling exists. Do not assume the old Hive `PARQUET.COMPRESS` value transfers (e.g. `SNAPPY` pre-migration does not imply `snappy` post-migration) and do not copy a codec from an unrelated example table. |
| `write.target-file-size-bytes` | Default — omit only with a stated reason | **Do not hardcode one number for the whole project.** First check the sibling project's convention. If none, reuse the `target-file-size-bytes` value already chosen for *this table's* `upd_iceberg_<table>.sql` compaction script (the maintenance templates in this guide default to `134217728` = 128 MiB) so write-time and compaction-time targets agree — picking different numbers for the two means compaction immediately starts rewriting freshly-written files. Scale up (256–512 MiB) for large append-heavy fact tables, down (64 MiB) for small slowly-growing dimension tables, based on existing parquet file sizes for that table (`hdfs dfs -du` or existing partition stats) — state the chosen size and why in the plan, don't silently pick a default. |
| `write.distribution-mode` | Default — omit only with a stated reason | `'none'` is the safe default when upstream already writes one file per partition reasonably. Switch to `'hash'` only if you observe many small files per partition in the existing parquet layout — confirm with the user first, since it changes write-time shuffle cost. |
| `write.metadata.delete-after-commit.enabled` | Default — omit only with a stated reason | `'true'` — without it `metadata.json` files accumulate forever; safe to always set. |
| `write.metadata.previous-versions-max` | Default — omit only with a stated reason | `'10'` — keep this numerically aligned with `app.sql.retain_snapshots` in the table's compaction wf (the maintenance templates in this guide also default to `10`); if you change one, change the other. |
| `write.bloom.filter.columns` | **Never default — set only when justified per table** | Set this **only** when the table has an obvious high-cardinality equality-lookup column: the join key used in a `MERGE INTO ... ON` writing to this table or the `sort_order` column already chosen for *this table's* `upd_iceberg_<table>.sql`. Derive it from that table's own DML/sort_order — never copy a column name from a different table's example (e.g. `ctl_validfrom` is only relevant if this table's compaction script actually sorts/filters by it). If no single column stands out as the dominant filter/join key, omit the property entirely rather than guessing. |
| `comment` | Default — omit only with a stated reason | Pull verbatim from the `Description` column of the S2T `Tables` sheet for this table. Never invent one. |

**Post-write sanity check:** after writing each table's DDL, count the `write.*`/`format-version`/`comment` keys actually present in the `TBLPROPERTIES` block. Fewer than 11 (`format-version` + the 3 `write.*.mode` keys + the other 7 "Default" rows including `comment` — i.e. everything except the one "Never default" row, `write.bloom.filter.columns`) means you likely fell back to the bare minimum — go back and fill in the missing defaults, or write down the specific reason you're omitting each one.
