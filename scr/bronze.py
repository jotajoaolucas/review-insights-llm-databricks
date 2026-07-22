"""
bronze.py — Camada Bronze do projeto review-insights-llm-databricks.

Ingestão bruta das avaliações: lê o CSV do Volume sem transformação de negócio,
sanitiza nomes de coluna (o Delta não aceita espaços/caracteres especiais) e
adiciona metadados de auditoria.
"""

import re
from pyspark.sql import functions as F

import config


def executar(spark):
    # leitura bruta — todas as colunas como string; opções de parsing para
    # texto livre com quebras de linha e aspas internas
    df = (spark.read
          .option("header", True)
          .option("inferSchema", False)
          .option("multiLine", True)
          .option("quote", '"')
          .option("escape", '"')
          .csv(f"{config.VOLUME_PATH}/{config.CSV_FILE}"))

    # sanitiza nomes de coluna (Delta rejeita ' ,;{}()\n\t=')
    novos_nomes = [re.sub(r"[ ,;{}()\n\t=]", "_", c) for c in df.columns]
    df = df.toDF(*novos_nomes)

    # metadados de auditoria
    df = (df
          .withColumn("_ingest_timestamp", F.current_timestamp())
          .withColumn("_source_file", F.lit(config.CSV_FILE)))

    df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{config.BRONZE_SCHEMA}.reviews")

    # validação: contagem tabela vs origem
    origem = (spark.read.option("header", True).option("multiLine", True)
              .option("quote", '"').option("escape", '"')
              .csv(f"{config.VOLUME_PATH}/{config.CSV_FILE}").count())
    tabela = spark.table(f"{config.BRONZE_SCHEMA}.reviews").count()
    assert origem == tabela, f"Contagem divergente: origem={origem}, tabela={tabela}"

    print(f"bronze.reviews: {tabela} linhas")
    return df
