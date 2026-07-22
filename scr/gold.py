"""
gold.py — Camada Gold do projeto review-insights-llm-databricks.

Produz a tabela principal (avaliações classificadas) e as tabelas agregadas
que sustentam as análises por assunto e sentimento.
"""

from pyspark.sql import functions as F

import config


def _reviews_classified(spark):
    (spark.table(f"{config.SILVER_SCHEMA}.reviews_enriched")
        .select("review_id", "texto_completo", "assunto", "sentimento",
                "rating", "positive_feedback_count", "age", "recommended",
                "clothing_id", "division_name", "department_name", "class_name")
        .filter(F.col("assunto").isNotNull() & (F.col("sentimento") != "indefinido"))
        .write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{config.GOLD_SCHEMA}.reviews_classified"))


def _agregacoes(spark):
    reviews = spark.table(f"{config.GOLD_SCHEMA}.reviews_classified")

    # matriz assunto x sentimento
    (reviews.groupBy("assunto", "sentimento")
        .agg(F.count("*").alias("qtd"))
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{config.GOLD_SCHEMA}.sentimento_por_assunto"))

    # resumo por assunto (com % de negativos)
    (reviews.groupBy("assunto")
        .agg(F.count("*").alias("total_reviews"),
             F.round(F.avg("rating"), 2).alias("rating_medio"),
             F.round(F.avg("age"), 1).alias("idade_media"),
             F.round(F.avg(F.when(F.col("sentimento") == "negativo", 1)
                           .otherwise(0)) * 100, 1).alias("pct_negativo"))
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{config.GOLD_SCHEMA}.assunto_resumo"))

    # sentimento por departamento (cruza com o produto oficial do dataset)
    (reviews.filter(F.col("department_name").isNotNull())
        .groupBy("department_name", "sentimento")
        .agg(F.count("*").alias("qtd"))
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{config.GOLD_SCHEMA}.sentimento_por_departamento"))


def executar(spark):
    _reviews_classified(spark)
    _agregacoes(spark)
    print("Camada Gold pronta: reviews_classified + agregações")
    print("(o resumo executivo via LLM fica em notebook próprio — 06_gold_resumo_executivo)")
