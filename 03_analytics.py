# -*- coding: utf-8 -*-
"""
Аналитика новостей Lenta.ru: динамика по месяцам, по рубрикам, сезонность.
Читает очищенные данные из HDFS, строит витрины, сохраняет в HDFS.
Запуск: spark-submit analytics.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("News Analytics") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --- Читаем очищенные данные и регистрируем как SQL-таблицу ---
news = spark.read.parquet("/user/hadoop/news/clean")
news.createOrReplaceTempView("news")

# --- Витрина 1: общая динамика по месяцам ---
monthly = spark.sql("""
    SELECT year, month, COUNT(*) AS articles
    FROM news
    GROUP BY year, month
    ORDER BY year, month
""")
monthly.write.mode("overwrite").parquet("/user/hadoop/news/mart_monthly_total")
print("Витрина mart_monthly_total сохранена")

# --- Витрина 2: динамика топ-рубрик по месяцам ---
topic_monthly = spark.sql("""
    SELECT year, month, topic, COUNT(*) AS articles
    FROM news
    WHERE topic IN ('Россия', 'Мир', 'Экономика', 'Спорт', 'Бывший СССР')
    GROUP BY year, month, topic
    ORDER BY year, month, topic
""")
topic_monthly.write.mode("overwrite").parquet("/user/hadoop/news/mart_topic_monthly")
print("Витрина mart_topic_monthly сохранена")

# --- Витрина 3: сезонность (среднее по месяцам, без неполного 2019) ---
seasonality = spark.sql("""
    SELECT month,
           ROUND(AVG(monthly_cnt), 0) AS avg_articles
    FROM (
        SELECT year, month, COUNT(*) AS monthly_cnt
        FROM news
        WHERE year >= 2020
        GROUP BY year, month
    )
    GROUP BY month
    ORDER BY month
""")
seasonality.write.mode("overwrite").parquet("/user/hadoop/news/mart_seasonality")
print("Витрина mart_seasonality сохранена")

# --- Контрольный вывод ---
print("\n=== Динамика по месяцам ===")
monthly.show(60)
print("\n=== Сезонность ===")
seasonality.show()

print("\nВсе витрины аналитики сохранены.")
spark.stop()
