-- ============================================================
-- Шаг 0: Создание базы и таблиц в MariaDB под витрины
-- ============================================================
-- Запуск: mysql -u student -pstudent < 00_mariadb_setup.sql
-- ============================================================

CREATE DATABASE IF NOT EXISTS news_analytics
    CHARACTER SET utf8 COLLATE utf8_general_ci;

USE news_analytics;

CREATE TABLE IF NOT EXISTS monthly_total (
    year INT, month INT, articles INT
) CHARACTER SET utf8;

CREATE TABLE IF NOT EXISTS topic_monthly (
    year INT, month INT, topic VARCHAR(100), articles INT
) CHARACTER SET utf8;

CREATE TABLE IF NOT EXISTS seasonality (
    month INT, avg_articles DOUBLE
) CHARACTER SET utf8;

CREATE TABLE IF NOT EXISTS keywords_year (
    year INT, lemma VARCHAR(150), freq INT
) CHARACTER SET utf8;

SHOW TABLES;
