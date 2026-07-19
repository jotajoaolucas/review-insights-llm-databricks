# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 04 - Gold: reviews_classified
# MAGIC Tabela principal do projeto: cada review com assunto, sentimento e produto,
# MAGIC pronta para filtrar e cruzar.
# MAGIC - Entrada: `silver.reviews_enriched`
# MAGIC - Saída: `gold.reviews_classified`
# MAGIC
# MAGIC O embedding (vetor pesado) é removido aqui — já cumpriu seu papel na Silver
# MAGIC e continua disponível em `silver.reviews_embeddings` se precisar depois.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "voc_project"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Montar a tabela principal
# MAGIC Selecionamos as colunas de negócio (sem o embedding). Produto vem das colunas
# MAGIC originais do dataset (`department_name`, `class_name`), assunto e sentimento vêm do LLM.

# COMMAND ----------

reviews_classified = (spark.table(f"{SILVER_SCHEMA}.reviews_enriched")
    .select(
        "review_id",
        "texto_completo",
        "assunto",
        "sentimento",
        "rating",
        "positive_feedback_count",
        "age",
        "recommended",
        "clothing_id",
        "division_name",
        "department_name",
        "class_name",
    )
    # remove reviews que não puderam ser classificadas
    .filter(F.col("assunto").isNotNull() & (F.col("sentimento") != "indefinido"))
)

reviews_classified.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_SCHEMA}.reviews_classified")

print(f"✅ gold.reviews_classified: {reviews_classified.count()} reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação

# COMMAND ----------

df = spark.table(f"{GOLD_SCHEMA}.reviews_classified")

print("Colunas:", df.columns)
print("Total de reviews classificadas:", df.count())

# amostra do resultado final
display(df.select("texto_completo", "assunto", "sentimento", "rating", "department_name").limit(15))

# COMMAND ----------

# exemplo de filtro cruzado (o objetivo do projeto): assunto + sentimento
print("Exemplo: reviews NEGATIVAS de um assunto específico\n")

assunto_exemplo = df.filter(F.col("sentimento") == "negativo") \
    .groupBy("assunto").count().orderBy(F.desc("count")).first()["assunto"]

print(f"Assunto com mais negativos: {assunto_exemplo}\n")
display(
    df.filter((F.col("assunto") == assunto_exemplo) & (F.col("sentimento") == "negativo"))
      .select("texto_completo", "rating", "department_name")
      .limit(10)
)