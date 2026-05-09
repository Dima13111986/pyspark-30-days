# PySpark 30 Days — Learning Progress

## Загальна інформація

- **Початок курсу:** 2026-05-08
- **Середовище:** Microsoft Fabric (Lakehouse: `pyspark_learning_lh`)
- **GitHub repo:** https://github.com/Dima13111986/pyspark-30-days
- **Поточний день:** День 1 ✅ Завершено
- **Наступний день:** День 2 — Schema (явна vs виведена), типи даних, читання JSON

---

## Структура repository
pyspark-30-days/
├── .gitignore
├── README.md
├── learning-progress.md
├── data/
│   └── sample_sales.csv
└── fabric/                                ← Fabric Git integration
├── Readme.md
├── day-01-pyspark-start.Notebook/
│   ├── .platform
│   └── notebook-content.py
└── pyspark_learning_lh.Lakehouse/
├── .platform
├── alm.settings.json
├── lakehouse.metadata.json
└── shortcuts.metadata.json

---

## День 1 — PySpark старт: SparkSession, перший DataFrame, читання CSV

- **Дата:** 2026-05-08
- **Статус:** ✅ Завершено
- **Середовище:** Microsoft Fabric (Lakehouse: `pyspark_learning_lh`)
- **Dataset:** `data/sample_sales.csv` (8 рядків, 6 колонок)
- **Notebook:** `fabric/day-01-pyspark-start.Notebook/notebook-content.py`

### Що зроблено

- ✅ Створено GitHub repository `pyspark-30-days`
- ✅ Створено Lakehouse `pyspark_learning_lh` у Microsoft Fabric
- ✅ Завантажено `sample_sales.csv` у Lakehouse Files/
- ✅ Створено перший notebook `day-01-pyspark-start`
- ✅ Перевірено, що SparkSession готова автоматично в Fabric (`spark.version`)
- ✅ Прочитано CSV у DataFrame з `inferSchema=true`
- ✅ Викликано `show()`, `printSchema()`, `count()`
- ✅ Виконано базовий `select` для трьох колонок
- ✅ Зроблено базову перевірку null values через `col + count + when`
- ✅ Налаштовано Fabric Git integration з папкою `fabric/`
- ✅ Перший commit через Fabric Source control

### PySpark API використано

- `spark.version`, `spark.sparkContext.appName`
- `spark.read.option("header", "true").option("inferSchema", "true").csv()`
- `df.show()`, `df.show(5, truncate=False)`
- `df.printSchema()`, `df.count()`, `df.columns`
- `df.select()`, `df.filter()`
- `pyspark.sql.functions`: `col`, `count`, `when`
- `df.createOrReplaceTempView()` + `spark.sql()` (короткий перегляд)

### Spark SQL використано

- Створення temp view: `df.createOrReplaceTempView("sales")`
- Базовий SELECT з GROUP BY і ORDER BY

### Помилки і складні моменти

- ⚠️ **Encoding issue:** при першому читанні CSV кириличні імена відображались як `?????`. 
  - Причина: файл був збережений у Windows-1251, а Spark за замовчуванням читає UTF-8.
  - Розв'язання: пересохранити CSV у UTF-8 через VS Code (статус-бар → Save with Encoding → UTF-8).
  - Альтернатива: `.option("encoding", "windows-1251")` у `spark.read`.
- ⚠️ **Структура Git:** перший раз Fabric закомітив артефакти прямо в корінь repo. 
  - Розв'язання: Disconnect → видалити старі папки → Reconnect з полем **Git folder = `fabric`**.

### Ключові терміни (засвоєно)

| Українською | English | Що це |
|---|---|---|
| Сесія Spark | SparkSession | Точка входу в Spark, у Fabric створюється автоматично |
| Датафрейм | DataFrame | Розподілена таблиця з рядками і колонками |
| Схема | Schema | Опис колонок: назва + тип |
| Ліниве виконання | Lazy evaluation | Spark не виконує код до виклику action |
| Трансформація | Transformation | Опис змін (select, filter) — не виконується одразу |
| Дія | Action | Команда, що запускає виконання (show, count, collect) |
| Озеро-сховище | Lakehouse | Сховище у Fabric для файлів і Delta таблиць |

### Що повторити перед Днем 2

- Чим відрізняється `inferSchema=true` від явної schema (StructType)
- Чому `order_date` зчитується як `string`, а не як `date`
- Що таке lazy evaluation на практиці

### Commits

- `Initial commit` — створення repo
- `Update README.md`
- `Committing 2 items from workspace` (Fabric: Notebook + Lakehouse) × 2
- `Create learning-progress.md`

---

## План на Дні 2–5 (нагадування)

- **День 2:** Schema (явна vs виведена), типи даних, читання JSON
- **День 3:** select, filter, withColumn, column expressions
- **День 4:** Transformations vs actions, lazy evaluation, show/count/collect deep dive
- **День 5:** Читання Parquet, запис у різні формати, **міні-проєкт #1: PySpark Starter Notebook**

## День 2 — Explicit schema і JSON
- Дата: 2026-05-09
- Статус: Завершено
- Notebook: fabric/day-02-explicit-schema-json.ipynb
- Datasets: Files/data/sample_sales.csv, Files/data/customers_json
- PySpark навички:
  - StructType, StructField
  - StringType, IntegerType, DoubleType, DateType, BooleanType
  - .schema(...) при читанні CSV і JSON
  - dateFormat option для CSV
  - порівняння inferred vs explicit schema
  - createOrReplaceTempView + spark.sql
- Spark SQL: GROUP BY + CASE WHEN для базового data quality звіту
- Помилки: 
- Що повторити: коли nullable=False має сенс, як Spark зберігає JSON (папка, не файл)
- GitHub commit: feat: day 02 explicit schema and json reading

