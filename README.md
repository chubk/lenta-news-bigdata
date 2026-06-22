# Анализ динамики новостной повестки Lenta.ru (2019–2023)

Проект по дисциплине **«Большие данные»**, ЮФУ
Автор: **Чуб Кирилл Сергеевич**

---

## Описание

Пакетный анализ архива новостей Lenta.ru за 2019–2023 годы (≈ 496 тыс. статей)
средствами стека Big Data. Выявление динамики тематических рубрик, сезонности
публикаций и трендов ключевых слов с визуализацией в Power BI.

## Технологический стек

`HDFS` · `YARN` · `Apache Spark` (PySpark, DataFrame API, Spark SQL) ·
`pymorphy2` (лемматизация) · `Apache Hive` · `Apache Sqoop` · `MariaDB` · `Power BI`

Платформа: CentOS 7 на виртуальной машине.

## Архитектура пайплайна

```
CSV (исходник)
  └─> HDFS                       хранение сырых данных
       └─> Spark                 очистка, типизация → Parquet
            └─> Spark SQL + pymorphy2   аналитика, витрины
                 ├─> Hive        SQL-слой поверх HDFS
                 └─> Sqoop       экспорт витрин в MariaDB
                      └─> MariaDB   реляционные витрины
                           └─> Power BI   дашборд
```

## Порядок запуска

> Предварительно: кластер поднят (`start-dfs.sh`, `start-yarn.sh`),
> датасет лежит в `~/news_project/data/`.

| № | Команда | Назначение |
|---|---------|------------|
| 0 | `mysql -u student -pstudent < 00_mariadb_setup.sql` | Создание базы и таблиц в MariaDB |
| 1 | `bash 01_load_to_hdfs.sh` | Загрузка исходного CSV в HDFS |
| 2 | `spark-submit 02_clean.py` | Очистка, типизация, фильтрация 2019–2023 → Parquet |
| 3 | `spark-submit 03_analytics.py` | Витрины: динамика по месяцам, по рубрикам, сезонность |
| 4 | `spark-submit 04_keywords.py` | Лемматизация заголовков, частоты слов по годам |
| 5 | `spark-submit 05_export_csv.py` | Конвертация витрин Parquet → CSV для Sqoop |
| 6 | `bash 06_sqoop_export.sh` | Экспорт витрин из HDFS в MariaDB |
| 7 | `hive -f 07_hive_setup.sql` | Внешние таблицы Hive + проверочные запросы |
| 8 | — | Power BI: подключение к витринам, построение дашборда |

Скрипты `spark-submit` запускаются с параметрами:
`--master yarn --driver-memory 2g --executor-memory 4g --executor-cores 3 --num-executors 3`

## Витрины данных

В HDFS (`/user/hadoop/news/`) и в MariaDB (база `news_analytics`):

| Витрина | Содержание |
|---------|------------|
| `monthly_total` | общая динамика новостей по месяцам |
| `topic_monthly` | динамика топ-5 рубрик по месяцам |
| `seasonality` | средний объём публикаций по месяцам года |
| `keywords_year` | частоты лемматизированных слов по годам |

## Датасет

[News dataset from Lenta.Ru, 2019–2023](https://www.kaggle.com/datasets/marialevchenko/news-dataset-from-lenta-ru-2019-2023) (Kaggle)

- Период: декабрь 2019 — декабрь 2023
- Объём: ≈ 496 тыс. статей, 1.23 ГБ, UTF-8
- Поля: `url`, `title`, `text`, `topic`, `tags`, `date`

## Ограничения данных

- **2019 год** представлен только декабрём — не репрезентативен в межгодовых сравнениях.
- **Май 2023** отсутствует, апрель и июнь 2023 недозаполнены (особенность исходного сбора).
- **Сезонность**: минимум — январь, пик — ноябрь; летнего спада не выявлено.
- Автоматическая лемматизация (pymorphy2) даёт единичные ошибки разбора.

## Структура репозитория

```
00_mariadb_setup.sql    создание базы и таблиц MariaDB
01_load_to_hdfs.sh      загрузка датасета в HDFS
02_clean.py             очистка и подготовка данных
03_analytics.py         аналитика: динамика, рубрики, сезонность
04_keywords.py          лемматизация и частоты слов
05_export_csv.py        конвертация витрин в CSV
06_sqoop_export.sh      экспорт в MariaDB через Sqoop
07_hive_setup.sql       таблицы Hive поверх HDFS
README.md               этот файл
```
