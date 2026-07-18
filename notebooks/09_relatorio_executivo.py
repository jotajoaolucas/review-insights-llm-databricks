# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 09 - Relatório Executivo (HTML)
# MAGIC Gera um relatório profissional em HTML com os resumos executivos e as análises,
# MAGIC pronto para visualizar, salvar no Volume e (depois) enviar por email.
# MAGIC - Entradas: `gold.resumos_executivos`, `gold.assunto_resumo`, `gold.sentimento_por_assunto`
# MAGIC - Saída: arquivo HTML no Volume

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

CATALOG = "voc_project"
GOLD_SCHEMA = f"{CATALOG}.gold"
OUTPUT_PATH = "/Volumes/voc_project/bronze/raw_files"   # onde o HTML será salvo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregar os dados das tabelas Gold

# COMMAND ----------

resumos = spark.table(f"{GOLD_SCHEMA}.resumos_executivos").orderBy(F.desc("qtd_negativos")).collect()
assunto_resumo = spark.table(f"{GOLD_SCHEMA}.assunto_resumo").orderBy(F.desc("pct_negativo")).collect()
 
total_reviews = spark.table(f"{GOLD_SCHEMA}.reviews_classified").count()
total_negativos = spark.table(f"{GOLD_SCHEMA}.reviews_classified").filter(F.col("sentimento") == "negativo").count()
pct_negativo_geral = round(total_negativos / total_reviews * 100, 1) if total_reviews else 0
 
print(f"Reviews: {total_reviews} | Negativas: {total_negativos} ({pct_negativo_geral}%)")
print(f"Assuntos resumidos: {len(resumos)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Montar o HTML

# COMMAND ----------

def resumo_para_html(texto):
    """Converte o texto do resumo (com bullets em - ou *) em HTML simples."""
    linhas = [l.strip() for l in texto.split("\n") if l.strip()]
    html = ""
    dentro_lista = False
    for l in linhas:
        if l.startswith("-") or l.startswith("*") or l.startswith("•"):
            if not dentro_lista:
                html += "<ul>"
                dentro_lista = True
            html += f"<li>{l.lstrip('-*• ').strip()}</li>"
        else:
            if dentro_lista:
                html += "</ul>"
                dentro_lista = False
            html += f"<p>{l}</p>"
    if dentro_lista:
        html += "</ul>"
    return html
 
 
data_hoje = datetime.now().strftime("%d/%m/%Y")
 
# --- linhas da tabela de assuntos ---
linhas_tabela = ""
for r in assunto_resumo:
    cor = "#c0392b" if r["pct_negativo"] >= 50 else ("#e67e22" if r["pct_negativo"] >= 25 else "#27ae60")
    linhas_tabela += f"""
      <tr>
        <td>{r['assunto']}</td>
        <td style="text-align:center">{r['total_reviews']}</td>
        <td style="text-align:center">{r['rating_medio']}</td>
        <td style="text-align:center">{r['idade_media']}</td>
        <td style="text-align:center; color:{cor}; font-weight:bold">{r['pct_negativo']}%</td>
      </tr>"""
 
# --- blocos de resumo executivo ---
blocos_resumo = ""
for r in resumos:
    blocos_resumo += f"""
      <div class="card">
        <div class="card-header">
          <span class="assunto">{r['assunto'].upper()}</span>
          <span class="badge">{r['qtd_negativos']} avaliações negativas</span>
        </div>
        <div class="card-body">{resumo_para_html(r['resumo'])}</div>
      </div>"""
 
html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<title>Relatório de Voz do Cliente</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; background:#f4f5f7;
         color:#2c3e50; margin:0; padding:0; }}
  .container {{ max-width:860px; margin:0 auto; background:#fff; }}
  .header {{ background:#1a2332; color:#fff; padding:40px; }}
  .header h1 {{ margin:0 0 8px 0; font-size:26px; }}
  .header p {{ margin:0; color:#9aa7b8; font-size:14px; }}
  .kpis {{ display:flex; padding:24px 40px; gap:20px; border-bottom:1px solid #eee; }}
  .kpi {{ flex:1; text-align:center; }}
  .kpi .num {{ font-size:28px; font-weight:bold; color:#1a2332; }}
  .kpi .lab {{ font-size:12px; color:#7f8c8d; text-transform:uppercase; }}
  .section {{ padding:30px 40px; }}
  .section h2 {{ font-size:18px; border-left:4px solid #e67e22; padding-left:12px; }}
  table {{ width:100%; border-collapse:collapse; font-size:14px; }}
  th, td {{ padding:10px; border-bottom:1px solid #eee; text-align:left; }}
  th {{ background:#f8f9fa; color:#7f8c8d; text-transform:uppercase; font-size:12px; }}
  .card {{ border:1px solid #eee; border-radius:8px; margin-bottom:16px; overflow:hidden; }}
  .card-header {{ background:#f8f9fa; padding:12px 16px; display:flex;
                  justify-content:space-between; align-items:center; }}
  .assunto {{ font-weight:bold; color:#1a2332; }}
  .badge {{ background:#c0392b; color:#fff; padding:3px 10px; border-radius:12px; font-size:12px; }}
  .card-body {{ padding:16px; font-size:14px; }}
  .card-body ul {{ margin:8px 0; padding-left:20px; }}
  .card-body li {{ margin-bottom:6px; }}
  .footer {{ padding:24px 40px; color:#95a5a6; font-size:12px; border-top:1px solid #eee; }}
</style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>Relatório de Voz do Cliente</h1>
      <p>Análise automatizada de avaliações · {data_hoje}</p>
    </div>
 
    <div class="kpis">
      <div class="kpi"><div class="num">{total_reviews}</div><div class="lab">Avaliações</div></div>
      <div class="kpi"><div class="num">{total_negativos}</div><div class="lab">Negativas</div></div>
      <div class="kpi"><div class="num">{pct_negativo_geral}%</div><div class="lab">Taxa negativa</div></div>
      <div class="kpi"><div class="num">{len(assunto_resumo)}</div><div class="lab">Assuntos</div></div>
    </div>
 
    <div class="section">
      <h2>Assuntos por criticidade</h2>
      <table>
        <tr><th>Assunto</th><th style="text-align:center">Reviews</th>
            <th style="text-align:center">Rating médio</th>
            <th style="text-align:center">Idade média</th>
            <th style="text-align:center">% negativo</th></tr>
        {linhas_tabela}
      </table>
    </div>
 
    <div class="section">
      <h2>Resumos executivos — principais problemas e ações</h2>
      {blocos_resumo}
    </div>
 
    <div class="footer">
      Gerado automaticamente a partir de {total_reviews} avaliações usando classificação por LLM.<br>
      Projeto: review-insights-llm-databricks
    </div>
  </div>
</body>
</html>"""
 
print("✅ HTML montado:", len(html), "caracteres")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizar o relatório no notebook

# COMMAND ----------

displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar o relatório como arquivo no Volume

# COMMAND ----------

caminho_fixo = f"{OUTPUT_PATH}/relatorio_voc.html"
with open(caminho_fixo, "w", encoding="utf-8") as f:
    f.write(html)
print(f"✅ Relatório (nome fixo) salvo em: {caminho_fixo}")
 
# cópia datada — histórico
caminho_datado = f"{OUTPUT_PATH}/relatorio_voc_{datetime.now().strftime('%Y%m%d')}.html"
with open(caminho_datado, "w", encoding="utf-8") as f:
    f.write(html)
print(f"✅ Cópia datada salva em: {caminho_datado}")
 
print("\nO GitHub Actions baixa 'relatorio_voc.html' automaticamente via API.")