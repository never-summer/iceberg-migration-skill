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
  where => 'part_report_dt >= ${app.sql.service_date_from}',
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
  where => 'part_report_dt >= ${app.sql.service_date_from}',
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
  where => 'part_report_dt >= ${app.sql.service_date_from}',
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
