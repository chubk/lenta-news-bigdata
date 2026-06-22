-- ============================================================
-- Шаг 7: Создание внешних таблиц Hive поверх витрин в HDFS
-- ============================================================
-- Запуск: hive -f 07_hive_setup.sql
-- ============================================================

CREATE DATABASE IF NOT EXISTS news;
USE news;

CREATE EXTERNAL TABLE IF NOT EXISTS topic_monthly (
    year INT, month INT, topic STRING, articles BIGINT
) STORED AS PARQUET
LOCATION '/user/hadoop/news/mart_topic_monthly';

CREATE EXTERNAL TABLE IF NOT EXISTS monthly_total (
    year INT, month INT, articles BIGINT
) STORED AS PARQUET
LOCATION '/user/hadoop/news/mart_monthly_total';

CREATE EXTERNAL TABLE IF NOT EXISTS seasonality (
    month INT, avg_articles DOUBLE
) STORED AS PARQUET
LOCATION '/user/hadoop/news/mart_seasonality';

CREATE EXTERNAL TABLE IF NOT EXISTS keywords_year (
    year INT, lemma STRING, freq BIGINT
) STORED AS PARQUET
LOCATION '/user/hadoop/news/mart_keywords_year';

-- Проверочные аналитические запросы
SELECT 'Топ-10 слов 2022:' AS info;
SELECT lemma, freq FROM keywords_year WHERE year=2022 ORDER BY freq DESC LIMIT 10;

SELECT 'Объём новостей по годам:' AS info;
SELECT year, SUM(articles) AS total FROM monthly_total GROUP BY year ORDER BY year;
