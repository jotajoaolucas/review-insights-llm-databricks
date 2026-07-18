# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 05 - Silver: reviews_enriched
# MAGIC Classifica cada review em **ASSUNTO + SENTIMENTO** numa única chamada de LLM (JSON).
# MAGIC - Entrada: `silver.reviews_embeddings`
# MAGIC - Saída: `silver.reviews_enriched`
# MAGIC
# MAGIC Nota: produto/departamento não é classificado por LLM — já existe nas colunas originais do dataset.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "voc_project"
SILVER_SCHEMA = f"{CATALOG}.silver"

LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classificação: assunto + sentimento (uma chamada, retorno em JSON)

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER_SCHEMA}.reviews_classified_raw AS
    SELECT
        *,
        ai_query(
            '{LLM_ENDPOINT}',
            CONCAT(
                'Analise esta avaliação de roupa e responda APENAS com um JSON válido, sem texto extra, no formato: ',
                '{{"assunto": "...", "sentimento": "..."}}. ',
                'Regras: ',
                '"assunto" = o tema principal do comentário, geralmente um problema ou aspecto avaliado ',
                '(ex: caimento, tamanho, qualidade do tecido, cor, conforto, defeito, preço). Use 1 a 3 palavras. ',
                '"sentimento" = positivo, negativo ou neutro. ',
                'Responda em português. Avaliação: ',
                texto_completo
            )
        ) AS classificacao_json
    FROM {SILVER_SCHEMA}.reviews_embeddings
""")

print("✅ Classificação bruta (JSON) gerada")
display(spark.table(f"{SILVER_SCHEMA}.reviews_classified_raw").select("texto_completo", "classificacao_json").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extrair campos do JSON e normalizar sentimento

# COMMAND ----------

df = spark.table(f"{SILVER_SCHEMA}.reviews_classified_raw")

df_extraido = (df
    .withColumn("assunto", F.lower(F.trim(F.get_json_object(F.col("classificacao_json"), "$.assunto"))))
    .withColumn("sentimento", F.lower(F.trim(F.get_json_object(F.col("classificacao_json"), "$.sentimento"))))
    # normaliza sentimento pras 3 categorias
    .withColumn("sentimento",
        F.when(F.col("sentimento").contains("positiv"), "positivo")
         .when(F.col("sentimento").contains("negativ"), "negativo")
         .when(F.col("sentimento").contains("neutr"), "neutro")
         .otherwise("indefinido"))
)

df_extraido.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.reviews_enriched")

print("✅ silver.reviews_enriched gravada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação

# COMMAND ----------

df_final = spark.table(f"{SILVER_SCHEMA}.reviews_enriched")

# Quantos ficaram indefinidos (JSON mal formado)
indefinidos = df_final.filter(F.col("sentimento") == "indefinido").count()
sem_assunto = df_final.filter(F.col("assunto").isNull()).count()
print(f"Sentimento indefinido: {indefinidos}")
print(f"Sem assunto: {sem_assunto}")

print("\nDistribuição de sentimento:")
df_final.groupBy("sentimento").count().orderBy(F.desc("count")).show()

print("Top assuntos:")
df_final.groupBy("assunto").count().orderBy(F.desc("count")).show(15, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação cruzada (sanity check)
# MAGIC Sentimento negativo deveria ter rating médio baixo — se crescer de negativo→positivo, o LLM está coerente.

# COMMAND ----------

print("Rating médio por sentimento (deve crescer negativo→positivo):")
df_final.groupBy("sentimento") \
    .agg(F.round(F.avg("rating"), 2).alias("rating_medio"),
         F.count("*").alias("qtd")) \
    .orderBy("rating_medio") \
    .show()