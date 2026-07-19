# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 05 - Gold: Agregações
# MAGIC Tabelas de análise, prontas para filtrar e alimentar o dashboard.
# MAGIC - Entrada: `gold.reviews_classified`
# MAGIC - Saídas:
# MAGIC   - `gold.sentimento_por_assunto` — matriz assunto × sentimento
# MAGIC   - `gold.assunto_resumo` — volume, rating e % negativo por assunto
# MAGIC   - `gold.sentimento_por_departamento` — cruzamento com o tipo de produto

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "voc_project"
GOLD_SCHEMA = f"{CATALOG}.gold"

reviews = spark.table(f"{GOLD_SCHEMA}.reviews_classified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Matriz assunto × sentimento
# MAGIC O coração da análise: qual assunto concentra qual sentimento.

# COMMAND ----------

sentimento_por_assunto = (reviews
    .groupBy("assunto", "sentimento")
    .agg(F.count("*").alias("qtd"))
    .orderBy("assunto", "sentimento")
)

sentimento_por_assunto.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_SCHEMA}.sentimento_por_assunto")

print(f"✅ gold.sentimento_por_assunto")
display(sentimento_por_assunto)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Resumo por assunto
# MAGIC Volume, rating médio, idade média e % de negativos — assuntos mais problemáticos no topo.

# COMMAND ----------

assunto_resumo = (reviews
    .groupBy("assunto")
    .agg(
        F.count("*").alias("total_reviews"),
        F.round(F.avg("rating"), 2).alias("rating_medio"),
        F.round(F.avg("age"), 1).alias("idade_media"),
        F.round(F.avg(F.when(F.col("sentimento") == "negativo", 1).otherwise(0)) * 100, 1).alias("pct_negativo")
    )
    .orderBy(F.desc("pct_negativo"))
)

assunto_resumo.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_SCHEMA}.assunto_resumo")

print(f"✅ gold.assunto_resumo")
display(assunto_resumo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sentimento por departamento (produto)
# MAGIC Cruza o assunto/sentimento com o tipo de produto oficial do dataset.

# COMMAND ----------

sentimento_por_departamento = (reviews
    .filter(F.col("department_name").isNotNull())
    .groupBy("department_name", "sentimento")
    .agg(F.count("*").alias("qtd"))
    .orderBy("department_name", "sentimento")
)

sentimento_por_departamento.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_SCHEMA}.sentimento_por_departamento")

print(f"✅ gold.sentimento_por_departamento")
display(sentimento_por_departamento)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação

# COMMAND ----------

# percentuais de sentimento por assunto devem somar ~100%
from pyspark.sql import Window

total_por_assunto = (spark.table(f"{GOLD_SCHEMA}.sentimento_por_assunto")
    .withColumn("total", F.sum("qtd").over(Window.partitionBy("assunto")))
    .withColumn("pct", F.round(F.col("qtd") / F.col("total") * 100, 1))
)

print("Distribuição percentual de sentimento dentro de cada assunto:")
display(total_por_assunto.orderBy("assunto", "sentimento"))