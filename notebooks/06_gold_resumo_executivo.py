# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 06 - Gold: Resumos Executivos Automáticos
# MAGIC Para cada assunto crítico, o LLM lê as avaliações negativas e sintetiza os
# MAGIC principais problemas em pontos acionáveis — voice of customer automatizado.
# MAGIC - Entrada: `gold.reviews_classified`
# MAGIC - Saída: `gold.resumo_executivo`

# COMMAND ----------

from pyspark.sql import functions as F
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

CATALOG = "voc_project"
GOLD_SCHEMA = f"{CATALOG}.gold"

LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Selecionar os assuntos mais críticos
# MAGIC Os assuntos com mais avaliações negativas — onde vale gerar um resumo.

# COMMAND ----------

assuntos_criticos = (spark.table(f"{GOLD_SCHEMA}.reviews_classified")
    .filter(F.col("sentimento") == "negativo")
    .groupBy("assunto")
    .agg(F.count("*").alias("qtd_negativos"))
    .orderBy(F.desc("qtd_negativos"))
    .limit(5)                      # top 5 assuntos mais problemáticos
    .collect()
)

print("Assuntos que serão resumidos:")
for row in assuntos_criticos:
    print(f"  • {row['assunto']}: {row['qtd_negativos']} avaliações negativas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função que gera o resumo de um assunto

# COMMAND ----------

def gerar_resumo(assunto, max_reviews=15):
    reviews = (spark.table(f"{GOLD_SCHEMA}.reviews_classified")
        .filter((F.col("assunto") == assunto) & (F.col("sentimento") == "negativo"))
        .select("texto_completo")
        .limit(max_reviews)
        .collect()
    )
    textos = "\n- ".join([r["texto_completo"] for r in reviews])

    prompt = (
        f"Você é um analista de voz do cliente. Abaixo estão avaliações NEGATIVAS "
        f"de clientes sobre o assunto '{assunto}' em uma loja de roupas.\n\n"
        f"Avaliações:\n- {textos}\n\n"
        f"Sintetize em no máximo 3 bullets os principais problemas relatados. "
        f"Depois, sugira 1 ação concreta para a empresa. "
        f"Responda em português, de forma objetiva e executiva."
    )

    resposta = w.serving_endpoints.query(
        name=LLM_ENDPOINT,
        messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
        max_tokens=300
    )
    return resposta.choices[0].message.content

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gerar os resumos

# COMMAND ----------

resumos = []
for row in assuntos_criticos:
    assunto = row["assunto"]
    print(f"\n{'='*60}")
    print(f"ASSUNTO: {assunto.upper()} ({row['qtd_negativos']} avaliações negativas)")
    print('='*60)
    resumo = gerar_resumo(assunto)
    print(resumo)
    resumos.append({
        "assunto": assunto,
        "qtd_negativos": int(row["qtd_negativos"]),
        "resumo": resumo,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar como tabela Gold

# COMMAND ----------

df_resumos = spark.createDataFrame(resumos)

df_resumos.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_SCHEMA}.resumo_executivo")

print("✅ gold.resumo_executivo gravada")
display(spark.table(f"{GOLD_SCHEMA}.resumo_executivo"))