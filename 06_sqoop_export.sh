#!/bin/bash
# ============================================================
# Шаг 6: Экспорт витрин из HDFS (CSV) в MariaDB через Sqoop
# ============================================================
# Предполагается, что:
#  - база news_analytics и таблицы уже созданы в MariaDB
#  - CSV-витрины созданы скриптом 05_export_csv.py
# Запуск: bash 06_sqoop_export.sh
# ============================================================

CONN="jdbc:mysql://localhost/news_analytics?useUnicode=true&characterEncoding=UTF-8"
USER="student"
PASS="student"

export_table () {
    TABLE=$1
    DIR=$2
    echo "=== Экспорт $TABLE ==="
    sqoop export \
        --connect "$CONN" \
        --username $USER --password $PASS \
        --table $TABLE \
        --export-dir /user/hadoop/news/$DIR \
        --input-fields-terminated-by ',' \
        -m 1
}

export_table monthly_total  csv_monthly_total
export_table topic_monthly  csv_topic_monthly
export_table seasonality    csv_seasonality
export_table keywords_year  csv_keywords_year

echo "Все витрины экспортированы в MariaDB."
