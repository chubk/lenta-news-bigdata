# -*- coding: utf-8 -*-
"""
Шаг 2: Очистка и подготовка данных.
Читает сырой CSV из HDFS, чистит, типизирует, добавляет year/month,
сохраняет в Parquet (/user/hadoop/news/clean).
Запуск: spark-submit 02_clean.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month

spark = SparkSession.builder.appName("News Clean").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Читаем сырой CSV с правильными опциями для многострочных текстов
df = spark.read \
    .option("header", True) \
    .option("multiLine", True) \
    .option("escape", '"') \
    .csv("/user/hadoop/news/raw/lenta_ru_news_2019_2023.csv")

print("Исходное число записей:", df.count())

# Очистка: убираем пустые topic/title, типизируем дату, фильтруем 2019-2023
df_clean = df.filter(col("topic").isNotNull() & col("title").isNotNull()) \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .filter((year(col("date")) >= 2019) & (year(col("date")) <= 2023)) \
    .withColumn("year", year(col("date"))) \
    .withColumn("month", month(col("date")))

print("После очистки:", df_clean.count())

# Сохраняем в Parquet
df_clean.write.mode("overwrite").parquet("/user/hadoop/news/clean")
print("Очищенные данные сохранены в /user/hadoop/news/clean")

spark.stop()
