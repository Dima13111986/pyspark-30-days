# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "99807efd-25f9-40fb-9f3c-8e0b7aad760b",
# META       "default_lakehouse_name": "pyspark_learning_lh",
# META       "default_lakehouse_workspace_id": "873cd039-df6f-4d18-a5ef-35b0cca0b8a8",
# META       "known_lakehouses": [
# META         {
# META           "id": "99807efd-25f9-40fb-9f3c-8e0b7aad760b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# === Комірка 1: Перевірка SparkSession ===
# У Microsoft Fabric SparkSession уже створена автоматично.
# Змінна spark доступна одразу.

print(f"Spark version: {spark.version}")
print(f"App name: {spark.sparkContext.appName}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === Комірка 2: Читання CSV ===
# У Fabric шлях до файлу в Lakehouse має префікс "Files/"
# Це relative path до Lakehouse, який прикріплений до notebook

csv_path = "Files/data/sample_sales.csv"

df_sales = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("encoding", "windows-1251") \
    .csv(csv_path)

# Зверни увагу: тут НІЧОГО ще не виконалося насправді!
# Це lazy evaluation — Spark лише запам'ятав план.
print("DataFrame створено (план побудовано, але дані ще не прочитані)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === Комірка 3: Перші actions — show, schema, count ===
# Ось тепер Spark реально читає файл, бо ми викликаємо action

print("=== Перші 5 рядків ===")
df_sales.show(5)

print("=== Schema (схема) ===")
df_sales.printSchema()

row_count = df_sales.count()
print(f"=== Кількість рядків: {row_count} ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === Комірка 4: Перша мінімальна трансформація ===
# Виберемо тільки 3 колонки

df_simple = df_sales.select("order_id", "product", "price")
df_simple.show()

# Поки що це просто демонстрація — глибше підемо в Дні 3

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === Комірка 5: Базова перевірка якості — null values ===
from pyspark.sql.functions import col, count, when

# Скільки null у кожній колонці?
null_counts = df_sales.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in df_sales.columns
])
null_counts.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === Комірка 6 (опціонально): Перший Spark SQL ===
# Створюємо temporary view — таблицю, видиму тільки в цій сесії

df_sales.createOrReplaceTempView("sales")

# Тепер можна писати чистий SQL
result = spark.sql("""
    SELECT product, SUM(quantity) AS total_qty
    FROM sales
    GROUP BY product
    ORDER BY total_qty DESC
""")
result.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

---

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
