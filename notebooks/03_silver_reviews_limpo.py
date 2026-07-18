# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03 - Silver: reviews_limpo
# MAGIC Limpeza e padronização das avaliações brutas.
# MAGIC - Entrada: `bronze.reviews`
# MAGIC - Saída: `silver.reviews_limpo`

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "voc_project"
BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"

AMOSTRA = 3000   # nº de reviews a processar (controla custo/tempo das próximas etapas)

# COMMAND ----------

df = spark.table(f"{BRONZE_SCHEMA}.reviews")

# Renomear colunas (bronze gravou com underscore)
renomear = {
    "_c0": "review_id", "Clothing_ID": "clothing_id", "Age": "age", "Title": "title",
    "Review_Text": "review_text", "Rating": "rating", "Recommended_IND": "recommended",
    "Positive_Feedback_Count": "positive_feedback_count", "Division_Name": "division_name",
    "Department_Name": "department_name", "Class_Name": "class_name",
}
for antigo, novo in renomear.items():
    if antigo in df.columns:
        df = df.withColumnRenamed(antigo, novo)

# Tipagem
df = (df
    .withColumn("age", F.expr("try_cast(age as int)"))
    .withColumn("rating", F.expr("try_cast(rating as int)"))
    .withColumn("recommended", F.expr("try_cast(recommended as int)"))
    .withColumn("positive_feedback_count", F.expr("try_cast(positive_feedback_count as int)"))
)

# Remover reviews sem texto (não é possível embedar vazio)
antes = df.count()
df = df.filter(F.col("review_text").isNotNull() & (F.trim(F.col("review_text")) != ""))
print(f"Removidas {antes - df.count()} reviews sem texto")

# Campo único de texto (título + corpo)
df = df.withColumn(
    "texto_completo",
    F.trim(F.concat_ws(". ", F.coalesce(F.col("title"), F.lit("")), F.col("review_text")))
)

# Amostra
df_amostra = df.limit(AMOSTRA)
print(f"Amostra: {df_amostra.count()} reviews")

# COMMAND ----------

df_amostra.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.reviews_limpo")

print(f"✅ silver.reviews_limpo: {df_amostra.count()} reviews")

# COMMAND ----------

df_check = spark.table(f"{SILVER_SCHEMA}.reviews_limpo")
print("Colunas:", df_check.columns)
display(df_check.select("review_id", "texto_completo", "rating", "department_name").limit(5))