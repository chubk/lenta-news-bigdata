# -*- coding: utf-8 -*-
"""
Частотный анализ ключевых слов в заголовках новостей Lenta.ru.
Лемматизация через pymorphy2 (честный UDF), подсчёт частоты лемм по годам.
Запуск: spark-submit keywords.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, split, explode, regexp_replace, length, udf
)
from pyspark.sql.types import StringType

# --- 1. Создаём Spark-сессию ---
spark = SparkSession.builder \
    .appName("News Keywords Analysis") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")  # меньше мусора в логах

# --- 2. Список стоп-слов (служебные + частотный новостной мусор) ---
STOP_WORDS = {
    'и','в','во','не','что','он','на','я','с','со','как','а','то','все','она',
    'так','его','но','да','ты','к','у','же','вы','за','бы','по','только','ее',
    'мне','было','вот','от','меня','еще','нет','о','из','ему','теперь','когда',
    'даже','ну','вдруг','ли','если','уже','или','ни','быть','был','него','до',
    'вас','нибудь','опять','уж','вам','ведь','там','потом','себя','ничего','ей',
    'может','они','тут','где','есть','надо','ней','для','мы','тебя','их','чем',
    'была','сам','чтоб','без','будто','чего','раз','тоже','себе','под','будет',
    'тогда','кто','этот','того','потому','этого','какой','совсем','ним','здесь',
    'этом','один','почти','мой','тем','чтобы','нее','сейчас','были','куда','зачем',
    'всех','никогда','можно','при','наконец','два','об','другой','хоть','после',
    'над','больше','тот','через','эти','нас','про','всего','них','какая','много',
    'разве','три','эту','моя','впрочем','свою','этой','перед','лучше','чуть','том',
    'нельзя','такой','им','более','всегда','конечно','всю','между','стать','наш',
    'который','это','стало','этих','свой','весь','год','года','году','время',
    'мочь','новый','первый','назвать','рассказать','заявить','сообщить','назвал',
    'назвали','рассказал','рассказали','заявил','заявили','сообщил','стали','дать','раскрыть','оценить','объяснить','способ','известно','попасть','самый','видео',
    'предложить','призвать','глава','число','страна','регион','область','назвать'
}

# --- 3. Честный UDF: лемматизация с ленивой инициализацией ---
# Анализатор создаётся один раз на каждом воркере (через global),
# а не пересылается с драйвера и не пересоздаётся на каждое слово.
_morph = None

def lemmatize(word):
    global _morph
    if _morph is None:
        import pymorphy2
        _morph = pymorphy2.MorphAnalyzer()
    if word is None:
        return None
    return _morph.parse(word)[0].normal_form

lemmatize_udf = udf(lemmatize, StringType())

# --- 4. Читаем очищенные данные из HDFS ---
news = spark.read.parquet("/user/hadoop/news/clean")

# --- 5. Токенизация заголовков ---
words = news.select("year", lower(col("title")).alias("t")) \
    .withColumn("t", regexp_replace(col("t"), "[^а-яё ]", " ")) \
    .withColumn("word", explode(split(col("t"), "\\s+"))) \
    .filter(length(col("word")) >= 4)

# --- 6. Применяем лемматизацию ---
lemmas = words.withColumn("lemma", lemmatize_udf(col("word")))

# --- 7. Убираем стоп-слова (по леммам) и пустые ---
lemmas_clean = lemmas.filter(
    col("lemma").isNotNull() &
    (length(col("lemma")) >= 4) &
    (~col("lemma").isin(list(STOP_WORDS)))
)

# --- 8. Считаем частоту лемм по годам ---
lemma_year = lemmas_clean.groupBy("year", "lemma").count() \
    .withColumnRenamed("count", "freq")

# --- 9. Сохраняем витрину в HDFS ---
lemma_year.write.mode("overwrite").parquet("/user/hadoop/news/mart_keywords_year")

# --- 10. Показываем топ-20 лемм за каждый год для проверки ---
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

w = Window.partitionBy("year").orderBy(col("freq").desc())
top = lemma_year.withColumn("rn", row_number().over(w)).filter(col("rn") <= 20)

print("=== ТОП-20 ЛЕММ ПО ГОДАМ ===")
top.orderBy("year", "rn").show(100, truncate=False)

print("Готово. Витрина сохранена в /user/hadoop/news/mart_keywords_year")
spark.stop()
