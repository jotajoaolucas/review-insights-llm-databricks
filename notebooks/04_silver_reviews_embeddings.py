# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 04 - Silver: reviews_embeddings
# MAGIC Gera embeddings (vetor de significado) para cada review via Foundation Model API.
# MAGIC - Entrada: `silver.reviews_limpo`
# MAGIC - Saída: `silver.reviews_embeddings`

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "voc_project"
SILVER_SCHEMA = f"{CATALOG}.silver"

EMBEDDING_ENDPOINT = "databricks-gte-large-en"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gerar embeddings (em lote via ai_query)
# MAGIC Cada review vira um vetor de 1024 dimensões que captura seu significado.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER_SCHEMA}.reviews_embeddings AS
    SELECT
        *,
        ai_query('{EMBEDDING_ENDPOINT}', texto_completo) AS embedding
    FROM {SILVER_SCHEMA}.reviews_limpo
""")

print("✅ Embeddings gerados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação

# COMMAND ----------

df_emb = spark.table(f"{SILVER_SCHEMA}.reviews_embeddings")

exemplo = df_emb.select("embedding").first()["embedding"]
print("Dimensões do embedding:", len(exemplo))
print("Reviews com embedding:", df_emb.filter(F.col("embedding").isNotNull()).count())
print("Total de reviews:", df_emb.count())

display(df_emb.select("texto_completo", "embedding").limit(3))