# -*- coding: utf-8 -*-
"""
Конвертация витрин из Parquet в CSV для экспорта через Sqoop.
Каждая витрина сохраняется одним CSV-файлом без заголовка.
Запуск: spark-submit export_csv.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Export marts to CSV") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Список витрин: (папка Parquet, папка CSV)
marts = [
    ("mart_monthly_total", "csv_monthly_total"),
    ("mart_topic_monthly", "csv_topic_monthly"),
    ("mart_seasonality",   "csv_seasonality"),
    ("mart_keywords_year", "csv_keywords_year"),
]

for src, dst in marts:
    df = spark.read.parquet("/user/hadoop/news/" + src)
    # coalesce(1) — собрать в один файл; без заголовка (Sqoop не нужен header)
    df.coalesce(1).write.mode("overwrite") \
        .option("header", False) \
        .csv("/user/hadoop/news/" + dst)
    print("Сконвертировано: " + src + " -> " + dst)

print("Все витрины сохранены в CSV.")
spark.stop()
