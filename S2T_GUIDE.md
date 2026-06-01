# 📘 Инструкция по работе с S2T

## Содержание
1. [Что такое S2T](#что-такое-s2t)
2. [Структура проекта](#структура-проекта)
3. [Где хранятся файлы S2T](#где-хранятся-файлы-s2t)
4. [Как работают Gherkin-файлы](#как-работают-gherkin-файлы)
5. [Как добавить новую таблицу в S2T](#как-добавить-новую-таблицу-в-s2t)
6. [Как запустить проверку S2T](#как-запустить-проверку-s2t)

---

## Что такое S2T

**S2T (Source to Target)** — это спецификация source-to-target маппинга таблиц для Hadoop/Spark datamart-проектов.

### Архитектура
```
S2T Excel (спецификация)
    ↓
DDL Generator (генерация CREATE TABLE)
    ↓
SQL DDL файлы
    ↓
Tests (Cucumber/Gherkin сценарии)
```

### Типы S2T файлов

| Формат | Назначение | Пример |
|--------|------------|--------|
| `s2t.xlsx` | Спецификация таблиц (метаданные, колонки, типы) | `s2t.xlsx`, `hadoop_S2T_*.xlsx` |
| `*.feature` | Gherkin-сценарии тестирования | `ift.feature`, `st_skl.feature` |
| `auto_test.xlsx` | Автотесты (опционально) | `auto_test.xlsx` |

---

## Структура проекта

### Базовая структура datamart-проекта с S2T

```
src/main/resources/
├── s2t/                  # Файлы S2T
│   ├── s2t.xlsx         # Основная спецификация
│   └── qaapi/           # Автотесты
│       ├── ift.feature  # Функциональные тесты
│       └── st_skl.feature # Тесты спецификации
├── sql/
│   ├── ddl/             # DDL таблиц (создаются из S2T)
│   └── dml/             # DML скрипты
├── wf/
│   ├── ctl/             # Определения workflow (YAML)
│   └── oozie/           # Oozie workflow XML
└── devops/
    ├── devops.json      # Конфигурация деплоя
    └── devsecops.yml    # Безопасность
```

### devops.json — ключевые параметры для S2T

```json
{
  "datamart_name": "<project_name>",
  "s2t_confl_url": "",
  "distrib_files": [
    {
      "source": "s2t/s2t.xlsx",
      "destination": "s2t/"
    }
  ]
}
```

---

## Где хранятся файлы S2T

### Типичные имена файлов

```bash
# Варианты имен Excel-файлов:
s2t.xlsx
hadoop_S2T_<PROJECT>_v<version>.xlsx
```

### Расположение файлов

```
<PROJECT>/src/main/resources/s2t/
├── s2t.xlsx              # Основная спецификация
└── qaapi/                # Gherkin-сценарии
    ├── ift.feature       # Feature-файл для тестов
    ├── st_skl.feature    # Спецификация тестов
    └── *.feature         # Дополнительные сценарии
```

---

## Как работают Gherkin-файлы

### Структура .feature файла

```gherkin
# language: ru
Функционал: S2T_hadoop

  Предыстория:
    Дано Витрина <Название витрины>
    Пусть выбрана СУБД Hadoop
    И получен s2t "<имя_файла>"
    И выбрана среда Environment
    И s2t распарсен
    И s2t консистентна
    И задан ТУЗ <уникальный_идентификатор>
    И задана очередь ярн <queue_name>
    И получен DataSource

  Сценарий: DDL
    Когда произведены проверки DDL
    И отчет построен
    То DDL разработчика соответствуют S2T
```

### Пояснение параметров

| Параметр | Описание | Пример |
|----------|----------|--------|
| `Функционал` | Название функционала | `S2T_hadoop` |
| `Витрина` | Название витрины | `PFM Управление Расходами` |
| `s2t` | Имя Excel-файла спецификации | `s2t.xlsx`, `hadoop_S2T_SBSZH_v11.xlsx` |
| `ТУЗ` | Уникальный идентификатор (уникален в проекте) | `u_<unique_id>` |
| `очередь ярн` | Oozie queue | `root.<queue_name>` |

---

## Как добавить новую таблицу в S2T

### Шаг 1: Открой S2T Excel файл

```bash
<PROJECT>/src/main/resources/s2t/s2t.xlsx
# или
<PROJECT>/src/main/resources/s2t/hadoop_S2T_<project>_v<version>.xlsx
```

### Шаг 2: Добавь таблицу в лист

В Excel файле есть листы для разных объектов:

| Лист | Описание |
|------|----------|
| `Tables` | Список таблиц |
| `Columns` | Колонки таблиц |
| `Partitions` | Поля партиционирования |
| `Indexes` | Индексы |
| `Constraints` | Ограничения |

### Шаг 3: Заполни метаданные таблицы

**Лист `Tables`:**
```
| Table Name | Description | Storage | Location |
|------------|-------------|---------|----------|
| t_client_banker_binding_sua | Привязка банкиров к клиентам | HIVE | {{mart.hdfs_path_datamart}}/pa/t_client_banker_binding_sua |
```

**Лист `Columns`:**
```
| Table Name | Column Name | Data Type | Nullable | Description |
|------------|-------------|-----------|----------|-------------|
| t_client_banker_binding_sua | client_id | BIGINT | NO | ID клиента |
| t_client_banker_binding_sua | banker_id | BIGINT | NO | ID банкира |
| t_client_banker_binding_sua | start_dt | DATE | NO | Дата начала действия |
| t_client_banker_binding_sua | end_dt | DATE | YES | Дата окончания действия |
```

**Лист `Partitions`:**
```
| Table Name | Partition Column | Partition Type |
|------------|------------------|----------------|
| t_client_banker_binding_sua | ctl_loading | INT |
```

### Шаг 4: Добавь сценарий в Gherkin-файл

```gherkin
# language: ru
Функционал: S2T_hadoop

  Предыстория:
    Дано Витрина <Название витрины>
    Пусть выбрана СУБД Hadoop
    И получен s2t "s2t.xlsx"
    И выбрана среда Environment
    И s2t распарсен
    И s2t консистентна
    И задан ТУЗ u_<unique_id>
    И задана очередь ярн <queue_name>
    И получен DataSource

  # Существующие сценарии
  Сценарий: DDL
    Когда произведены проверки DDL
    И отчет построен
    То DDL разработчика соответствуют S2T

  # Новые сценарии для новой таблицы
  Сценарий: Данные в t_client_banker_binding_sua
    Когда загружены данные в t_client_banker_binding_sua
    И проверена целостность
    То количество записей > 0
```

---

## Как запустить проверку S2T

### Вариант 1: Через Maven (локально)

```bash
# Запуск проверки DDL
mvn test \
  -Ddatamart=<project_name> \
  -DrunTestsWithTags="SqlAttrCheck,SqlCheck" \
  -pl <path>/<datamart-modules>/<project_name> \
  -am
```

### Вариант 2: Через Oozie workflow

```yaml
# В wf/ctl/ctl.yml
- wf_schema_deploy:
    params:
      - param:
          name: spark_driver_extraJavaOptions
          prior_value: >
            "{{mart.spark_driver_extraJavaOptions}}
             -Dapp.hdfs.dir.path={{devops.datamart_path_app}}/sql/ddl
             -Dapp.s2t.resource.path={{mart.resource_path_s2t}}"
```

### Вариант 3: Через CI/CD

```bash
# В devops.json
"workflow_to_run": [
  "{{mart.wf_schema_deploy}}"
]

# В distrib_files
{
  "source": "s2t/s2t.xlsx",
  "destination": "s2t/"
}
```

---

## Стандартные переменные для S2T

### В mart.yml (для каждого проекта)

```yaml
# Путь к S2T файлу
resource_path_s2t: "/{{devops.datamart_name}}/s2t/s2t.xlsx"

# Параметры для сущностей (из 1_ctl_entities.yml)
# entity_id: <ID>
# path: "{{mart.hdfs_path_datamart}}/<layer>/<table_name>"
```

### В devops.json

```json
{
  "s2t_confl_url": "",  # URL Confluence для документации (опционально)
  "distrib_files": [
    {
      "source": "s2t/s2t.xlsx",
      "destination": "s2t/"
    }
  ]
}
```

---

## Контрольный список перед запуском

- [ ] S2T Excel файл создан и заполнен (`s2t.xlsx`)
- [ ] Gherkin-файлы созданы (`ift.feature`, `st_skl.feature`)
- [ ] `devops.json` содержит `distrib_files` для s2t
- [ ] `wf_schema_deploy` содержит `-Dapp.s2t.resource.path`
- [ ] `devops.json` содержит правильный `datamart_name`
- [ ] Проверены entity_id в `1_ctl_entities.yml`

---

## Типичные ошибки и решения

| Проблема | Причина | Решение |
|----------|---------|---------|
| `s2t file not found` | Файл не скопирован в HDFS | Проверь `distrib_files` в `devops.json` |
| `s2t inconsistent` | Несоответствие метаданных | Проверь листы `Tables`, `Columns`, `Partitions` |
| `DDL mismatch` | DDL не соответствует S2T | Обнови DDL или S2T Excel |
| `Entity not found` | Ошибка в entity_id | Проверь `1_ctl_entities.yml` |

---

## Полезные команды

```bash
# Найти s2t файлы во всех проектах
find . -name "s2t.xlsx" -o -name "hadoop_S2T_*.xlsx" 2>/dev/null

# Проверить структуру проекта
grep -rn "s2t.resource.path" src/main/resources/wf/ctl/

# Проверить distrib_files
grep -A 3 "s2t.xlsx" src/main/resources/devops/devops.json

# Найти Gherkin-файлы
find . -name "*.feature" 2>/dev/null
```

---

**Важно:** S2T — это **спецификация**, а не код. DDL генерируются автоматически на основе S2T Excel файла.
