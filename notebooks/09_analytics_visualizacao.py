# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 09 - Analytics e Visualização
# MAGIC Extração de insight visual a partir das camadas Gold e Silver.
# MAGIC - Mapa 2D semântico das avaliações (PCA sobre os embeddings, colorido por assunto)
# MAGIC - Matriz assunto × sentimento (heatmap)
# MAGIC - Ranking de "dor" (assuntos mais negativos)
# MAGIC - Exemplo de análise filtrada + insight de negócio
# MAGIC
# MAGIC Entradas: `silver.reviews_embeddings`, `gold.reviews_classified`,
# MAGIC `gold.assunto_resumo`, `gold.sentimento_por_assunto`

# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import numpy as np

CATALOG = "voc_project"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Mapa 2D semântico das avaliações
# MAGIC Os embeddings têm 1024 dimensões — impossíveis de visualizar direto.
# MAGIC Reduzimos para 2D com PCA para "enxergar" como as avaliações se agrupam.
# MAGIC Cada ponto é uma avaliação; a cor indica o assunto (classificado por LLM).
# MAGIC
# MAGIC Nota: o embedding fica em `silver.reviews_embeddings`; assunto/sentimento em
# MAGIC `gold.reviews_classified`. Juntamos os dois por `review_id`.

# COMMAND ----------

from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors, VectorUDT

df_emb = spark.table(f"{SILVER_SCHEMA}.reviews_embeddings").select("review_id", "embedding")
df_cls = spark.table(f"{GOLD_SCHEMA}.reviews_classified").select("review_id", "assunto", "sentimento")
df = df_emb.join(df_cls, "review_id")

# array -> Vector (formato que o PCA do MLlib espera)
array_to_vector = F.udf(lambda arr: Vectors.dense(arr), VectorUDT())
df = df.withColumn("features", array_to_vector(F.col("embedding")))

# PCA para 2 componentes
pca = PCA(k=2, inputCol="features", outputCol="pca_features")
modelo_pca = pca.fit(df)
df_pca = modelo_pca.transform(df)

print("✅ PCA aplicado (1024D → 2D)")

# COMMAND ----------

dados = df_pca.select("pca_features", "assunto").collect()

xs = [r["pca_features"][0] for r in dados]
ys = [r["pca_features"][1] for r in dados]
assuntos = [r["assunto"] for r in dados]

assuntos_unicos = sorted(set(assuntos))
cor_por_assunto = {a: plt.cm.tab20(i / max(len(assuntos_unicos), 1))
                   for i, a in enumerate(assuntos_unicos)}
cores = [cor_por_assunto[a] for a in assuntos]

plt.figure(figsize=(13, 9))
plt.scatter(xs, ys, c=cores, alpha=0.6, s=18)

handles = [plt.Line2D([0], [0], marker="o", color="w",
           markerfacecolor=cor_por_assunto[a], markersize=9, label=a)
           for a in assuntos_unicos]
plt.legend(handles=handles, title="Assunto", bbox_to_anchor=(1.02, 1),
           loc="upper left", fontsize=8)

plt.title("Mapa Semântico das Avaliações (PCA 2D)", fontsize=14)
plt.xlabel("Componente 1")
plt.ylabel("Componente 2")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Matriz Assunto × Sentimento (heatmap)
# MAGIC O coração da análise: qual assunto concentra qual sentimento.

# COMMAND ----------

matriz = (spark.table(f"{GOLD_SCHEMA}.sentimento_por_assunto")
    .groupBy("assunto")
    .pivot("sentimento")
    .agg(F.first("qtd"))
    .fillna(0)
    .orderBy("assunto")
    .toPandas()
    .set_index("assunto"))

plt.figure(figsize=(8, max(4, len(matriz) * 0.5)))
plt.imshow(matriz.values, cmap="RdYlGn_r", aspect="auto")
plt.xticks(range(len(matriz.columns)), matriz.columns)
plt.yticks(range(len(matriz.index)), matriz.index)
plt.colorbar(label="Nº de avaliações")

for i in range(len(matriz.index)):
    for j in range(len(matriz.columns)):
        plt.text(j, i, int(matriz.values[i, j]), ha="center", va="center", fontsize=9)

plt.title("Assunto × Sentimento")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ranking de "dor" por assunto
# MAGIC Assuntos ordenados pelo % de avaliações negativas — onde focar esforço.

# COMMAND ----------

resumo = (spark.table(f"{GOLD_SCHEMA}.assunto_resumo")
    .orderBy(F.desc("pct_negativo"))
    .toPandas())

plt.figure(figsize=(10, max(4, len(resumo) * 0.45)))
mediana = resumo["pct_negativo"].median()
cores_barra = ["#c0392b" if p > mediana else "#27ae60" for p in resumo["pct_negativo"]]
plt.barh(resumo["assunto"], resumo["pct_negativo"], color=cores_barra)
plt.xlabel("% de avaliações negativas")
plt.title("Ranking de Dor por Assunto")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()

display(spark.table(f"{GOLD_SCHEMA}.assunto_resumo").orderBy(F.desc("pct_negativo")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sentimento por departamento (produto)
# MAGIC Cruza o tema/sentimento com o tipo de produto oficial do dataset.

# COMMAND ----------

dep = (spark.table(f"{GOLD_SCHEMA}.sentimento_por_departamento")
    .groupBy("department_name")
    .pivot("sentimento")
    .agg(F.first("qtd"))
    .fillna(0)
    .toPandas()
    .set_index("department_name"))

dep.plot(kind="bar", stacked=True, figsize=(10, 5),
         color={"negativo": "#c0392b", "neutro": "#f39c12", "positivo": "#27ae60"})
plt.title("Sentimento por Departamento")
plt.ylabel("Nº de avaliações")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Exemplo de análise filtrada
# MAGIC O tipo de filtro para o qual o projeto foi desenhado: assunto + sentimento.

# COMMAND ----------

reviews = spark.table(f"{GOLD_SCHEMA}.reviews_classified")

assunto_top = (spark.table(f"{GOLD_SCHEMA}.assunto_resumo")
    .orderBy(F.desc("pct_negativo")).first()["assunto"])

print(f"Avaliações NEGATIVAS do assunto mais crítico: '{assunto_top}'\n")

display(
    reviews.filter((F.col("assunto") == assunto_top) & (F.col("sentimento") == "negativo"))
           .select("texto_completo", "rating", "age", "department_name")
           .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Insight de negócio

# COMMAND ----------

resumo = spark.table(f"{GOLD_SCHEMA}.assunto_resumo").orderBy(F.desc("pct_negativo")).toPandas()
total = spark.table(f"{GOLD_SCHEMA}.reviews_classified").count()
pior = resumo.iloc[0]

print("=" * 55)
print("INSIGHT DE NEGÓCIO")
print("=" * 55)
print(f"\nTotal de avaliações analisadas: {total}")
print(f"\nAssunto mais crítico: '{pior['assunto']}'")
print(f"  • {pior['pct_negativo']}% das avaliações são negativas")
print(f"  • Rating médio: {pior['rating_medio']}")
print(f"  • {int(pior['total_reviews'])} avaliações sobre este tema")
print(f"\nRecomendação: priorizar melhorias relacionadas a '{pior['assunto']}',")
print("o assunto que mais gera insatisfação nos clientes.")