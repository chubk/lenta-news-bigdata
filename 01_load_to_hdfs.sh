#!/bin/bash
# ============================================================
# Шаг 1: Загрузка исходного датасета Lenta.ru в HDFS
# ============================================================
# Предполагается, что CSV уже лежит в ~/news_project/data/
# Запуск: bash 01_load_to_hdfs.sh
# ============================================================

LOCAL_CSV=~/news_project/data/lenta_ru_news_2019_2023.csv
HDFS_RAW=/user/hadoop/news/raw

echo "Создаём папку в HDFS..."
hdfs dfs -mkdir -p $HDFS_RAW

echo "Загружаем CSV в HDFS..."
hdfs dfs -put -f $LOCAL_CSV $HDFS_RAW/

echo "Проверяем результат:"
hdfs dfs -ls -h $HDFS_RAW/

echo "Готово. Датасет загружен в HDFS."
