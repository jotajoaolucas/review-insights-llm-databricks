"""
silver.py — Camada Silver do projeto review-insights-llm-databricks.

Três etapas:
  1. Limpeza e padronização do texto
  2. Embeddings via Foundation Model API (vetor de significado por avaliação)
  3. Classificação de assunto + sentimento numa única chamada de LLM (JSON),
     com o assunto restrito a uma lista fechada de categorias.
"""

from pyspark.sql import functions as F

import config


def _limpeza(spark):
    df = spark.table(f"{config.BRONZE_SCHEMA}.reviews")

    for antigo, novo in config.RENOMEAR_COLUNAS.items():
        if antigo in df.columns:
            df = df.withColumnRenamed(antigo, novo)

    df = (df
          .withColumn("age", F.expr("try_cast(age as int)"))
          .withColumn("rating", F.expr("try_cast(rating as int)"))
          .withColumn("recommended", F.expr("try_cast(recommended as int)"))
          .withColumn("positive_feedback_count",
                      F.expr("try_cast(positive_feedback_count as int)")))

    df = df.filter(F.col("review_text").isNotNull()
                   & (F.trim(F.col("review_text")) != ""))

    df = df.withColumn(
        "texto_completo",
        F.trim(F.concat_ws(". ", F.coalesce(F.col("title"), F.lit("")),
                           F.col("review_text"))))

    df.limit(config.AMOSTRA).write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{config.SILVER_SCHEMA}.reviews_limpo")


def _embeddings(spark):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {config.SILVER_SCHEMA}.reviews_embeddings AS
        SELECT *, ai_query('{config.EMBEDDING_ENDPOINT}', texto_completo) AS embedding
        FROM {config.SILVER_SCHEMA}.reviews_limpo
    """)


def _classificacao(spark):
    # lista de assuntos formatada para o prompt (um por linha, com hífen)
    lista = "\\n".join(f"- {a}" for a in config.ASSUNTOS_VALIDOS)

    spark.sql(f"""
        CREATE OR REPLACE TABLE {config.SILVER_SCHEMA}.reviews_classified_raw AS
        SELECT *,
            ai_query('{config.LLM_ENDPOINT}',
                CONCAT(
                    'Você classifica avaliações de roupas. Responda APENAS com um JSON válido, ',
                    'sem texto extra, no formato: {{"assunto": "...", "sentimento": "..."}}.\\n\\n',
                    'O campo "assunto" DEVE ser EXATAMENTE um destes valores:\\n',
                    '{lista}\\n\\n',
                    'Regras: escolha UM único assunto da lista acima, exatamente como escrito ',
                    '(minúsculas, sem acento). Se não encaixar, use "outro". ',
                    'NÃO invente assuntos fora da lista. NÃO use o tipo de peça como assunto.\\n\\n',
                    'O campo "sentimento" deve ser: positivo, negativo ou neutro.\\n\\n',
                    'Avaliação: ', texto_completo)
            ) AS classificacao_json
        FROM {config.SILVER_SCHEMA}.reviews_embeddings
    """)

    df = spark.table(f"{config.SILVER_SCHEMA}.reviews_classified_raw")
    df = (df
          .withColumn("assunto",
                      F.lower(F.trim(F.get_json_object("classificacao_json", "$.assunto"))))
          .withColumn("sentimento",
                      F.lower(F.trim(F.get_json_object("classificacao_json", "$.sentimento"))))
          .withColumn("sentimento",
                      F.when(F.col("sentimento").contains("positiv"), "positivo")
                       .when(F.col("sentimento").contains("negativ"), "negativo")
                       .when(F.col("sentimento").contains("neutr"), "neutro")
                       .otherwise("indefinido")))

    df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{config.SILVER_SCHEMA}.reviews_enriched")


def executar(spark):
    _limpeza(spark)
    _embeddings(spark)
    _classificacao(spark)
    print("silver.reviews_enriched pronta (texto limpo + embedding + assunto + sentimento)")
