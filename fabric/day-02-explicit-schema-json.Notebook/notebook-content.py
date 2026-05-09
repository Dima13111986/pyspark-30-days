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
from pyspark.sql.types import(
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType
)

from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Описуємо схему явно.
# StructField(name, dataType, nullable)
sales_schema = StructType([
    StructField("order_id",      IntegerType(), nullable=False),
    StructField("customer_id",   IntegerType(), nullable=True),
    StructField("product",       StringType(),  nullable=True),
    StructField("quantity",      IntegerType(), nullable=True),
    StructField("price",         DoubleType(),  nullable=True),
    StructField("order_date",    DateType(),    nullable=True),
])

# 2. Читаємо CSV із явною схемою (без inferSchema!).
# У Fabric шлях до файлу в lakehouse — "Files/data/sample_sales.csv"
csv_path = "Files/data/sample_sales.csv"

df_sales = (
    spark.read
    .option("header", True)
    .option("dateFormat", "yyyy-MM-dd")  # формат дати у файлі
    .schema(sales_schema)
    .csv(csv_path)
)

# 3. Перевірки
df_sales.printSchema()
df_sales.show(truncate=False)
print(f"Кількість рядків: {df_sales.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Створюємо невеликий DataFrame з даними клієнтів і записуємо як JSON.
customers_data = [
    (1, "Olena Kovalenko", "olena@example.com", "Kyiv",   True),
    (2, "Andriy Shevchuk", "andriy@example.com", "Lviv",  True),
    (3, "Iryna Boyko",    None,                  "Odesa", False),
    (4, "Petro Melnyk",   "petro@example.com",   None,    True),
]

customers_columns = ["customer_id", "full_name", "email", "city", "is_active"]

df_customers_seed = spark.createDataFrame(customers_data, customers_columns)

# Записуємо як JSON у lakehouse.
# mode="overwrite" — щоб можна було перезапускати клітинку.
json_output_path = "Files/data/customers_json"
df_customers_seed.write.mode("overwrite").json(json_output_path)

print(f"JSON записаний у: {json_output_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import BooleanType

customers_schema = StructType([
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("full_name",   StringType(),  nullable=True),
    StructField("email",       StringType(),  nullable=True),
    StructField("city",        StringType(),  nullable=True),
    StructField("is_active",   BooleanType(), nullable=True),
])

df_customers = (
    spark.read
    .schema(customers_schema)
    .json("Files/data/customers_json")
)

df_customers.printSchema()
df_customers.show(truncate=False)
print(f"Кількість клієнтів: {df_customers.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Читаємо той самий JSON БЕЗ явної схеми (Spark угадає сам)
df_inferred = spark.read.json("Files/data/customers_json")

print("=== Inferred schema ===")
df_inferred.printSchema()

print("=== Explicit schema ===")
df_customers.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Скільки клієнтів без email?
missing_email = df_customers.filter(col("email").isNull()).count()
print(f"Клієнтів без email: {missing_email}")

# Скільки активних?
active_count = df_customers.filter(col("is_active") == True).count()
print(f"Активних клієнтів: {active_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_customers.createOrReplaceTempView("customers")

spark.sql("""
    SELECT
        city,
        COUNT(*) AS total_customers,
        SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_customers,
        SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS missing_email
    FROM customers
    GROUP BY city
    ORDER BY total_customers DESC
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
