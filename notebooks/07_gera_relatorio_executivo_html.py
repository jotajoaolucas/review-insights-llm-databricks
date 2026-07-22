# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Relatório Executivo (HTML)
# MAGIC Gera o relatório em HTML **compatível com clientes de email**.
# MAGIC
# MAGIC Nota técnica: Gmail, Outlook e afins ignoram CSS moderno (`flex`, `grid`) e boa parte
# MAGIC do que fica dentro de `<style>`. Por isso o layout usa **tabelas HTML** e **estilos inline** —
# MAGIC a técnica padrão para email. Fica visualmente igual, mas não "desmonta" na caixa de entrada.
# MAGIC
# MAGIC - Entradas: `gold.resumo_executivo`, `gold.assunto_resumo`, `gold.reviews_classified`
# MAGIC - Saída: `relatorio_voc.html` no Volume

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

CATALOG = "voc_project"
GOLD_SCHEMA = f"{CATALOG}.gold"
OUTPUT_PATH = "/Volumes/voc_project/bronze/raw_files"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregar os dados das tabelas Gold

# COMMAND ----------

resumos = spark.table(f"{GOLD_SCHEMA}.resumo_executivo").orderBy(F.desc("qtd_negativos")).collect()
assunto_resumo = spark.table(f"{GOLD_SCHEMA}.assunto_resumo").orderBy(F.desc("pct_negativo")).collect()

total_reviews = spark.table(f"{GOLD_SCHEMA}.reviews_classified").count()
total_negativos = spark.table(f"{GOLD_SCHEMA}.reviews_classified").filter(F.col("sentimento") == "negativo").count()
pct_negativo_geral = round(total_negativos / total_reviews * 100, 1) if total_reviews else 0

print(f"Reviews: {total_reviews} | Negativas: {total_negativos} ({pct_negativo_geral}%)")
print(f"Assuntos resumidos: {len(resumos)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Montar o HTML (layout em tabelas + estilos inline)

# COMMAND ----------

FONTE = "Arial,Helvetica,sans-serif"


def resumo_para_html(texto):
    """Converte o texto do resumo (bullets em - ou *) em HTML com estilos inline."""
    linhas = [l.strip() for l in texto.split("\n") if l.strip()]
    html = ""
    dentro_lista = False
    for l in linhas:
        if l.startswith(("-", "*", "•")):
            if not dentro_lista:
                html += '<ul style="margin:8px 0; padding-left:20px;">'
                dentro_lista = True
            html += f'<li style="margin-bottom:6px;">{l.lstrip("-*• ").strip()}</li>'
        else:
            if dentro_lista:
                html += "</ul>"
                dentro_lista = False
            html += f'<p style="margin:8px 0;">{l}</p>'
    if dentro_lista:
        html += "</ul>"
    return html


data_hoje = datetime.now().strftime("%d/%m/%Y")

# ---------------------------------------------------------------------------
# KPIs — como células de uma tabela (flex não funciona em email)
# ---------------------------------------------------------------------------
kpis = [
    (f"{total_reviews}", "Avaliações"),
    (f"{total_negativos}", "Negativas"),
    (f"{pct_negativo_geral}%", "Taxa negativa"),
    (f"{len(assunto_resumo)}", "Assuntos"),
]

kpi_cells = ""
for num, lab in kpis:
    kpi_cells += f"""
        <td align="center" valign="top" style="padding:14px 6px; font-family:{FONTE};">
          <div style="font-size:26px; font-weight:bold; color:#1a2332; line-height:1.2;">{num}</div>
          <div style="font-size:11px; color:#7f8c8d; text-transform:uppercase; letter-spacing:0.5px; padding-top:4px;">{lab}</div>
        </td>"""

# ---------------------------------------------------------------------------
# Tabela de assuntos por criticidade
# ---------------------------------------------------------------------------
linhas_tabela = ""
for r in assunto_resumo:
    cor = "#c0392b" if r["pct_negativo"] >= 50 else ("#e67e22" if r["pct_negativo"] >= 25 else "#27ae60")
    base = f'padding:10px; border-bottom:1px solid #eeeeee; font-family:{FONTE}; font-size:14px;'
    linhas_tabela += f"""
      <tr>
        <td style="{base} color:#2c3e50;">{r['assunto']}</td>
        <td align="center" style="{base} color:#2c3e50;">{r['total_reviews']}</td>
        <td align="center" style="{base} color:#2c3e50;">{r['rating_medio']}</td>
        <td align="center" style="{base} color:#2c3e50;">{r['idade_media']}</td>
        <td align="center" style="{base} color:{cor}; font-weight:bold;">{r['pct_negativo']}%</td>
      </tr>"""

th = f'padding:10px; background:#f8f9fa; font-family:{FONTE}; font-size:11px; color:#7f8c8d; text-transform:uppercase;'

# ---------------------------------------------------------------------------
# Cards de resumo executivo — header como tabela de 2 colunas
# (evita o badge sobrepor o título, que é o que acontece sem flex)
# ---------------------------------------------------------------------------
blocos_resumo = ""
for r in resumos:
    blocos_resumo += f"""
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
             style="border:1px solid #eeeeee; border-radius:8px; margin-bottom:16px; background:#ffffff;">
        <tr>
          <td style="background:#f8f9fa; padding:12px 16px; border-radius:8px 8px 0 0;">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
              <tr>
                <td align="left" style="font-family:{FONTE}; font-size:14px; font-weight:bold; color:#1a2332;">
                  {r['assunto'].upper()}
                </td>
                <td align="right" style="font-family:{FONTE}; font-size:12px; white-space:nowrap;">
                  <span style="background:#c0392b; color:#ffffff; padding:4px 10px; border-radius:12px; display:inline-block;">
                    {r['qtd_negativos']} avaliações negativas
                  </span>
                </td>
              </tr>
            </table>
          </td>
        </tr>
        <tr>
          <td style="padding:16px; font-family:{FONTE}; font-size:14px; color:#2c3e50; line-height:1.5;">
            {resumo_para_html(r['resumo'])}
          </td>
        </tr>
      </table>"""

# ---------------------------------------------------------------------------
# Documento final
# ---------------------------------------------------------------------------
html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Relatório de Voz do Cliente</title>
</head>
<body style="margin:0; padding:0; background:#f4f5f7;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f4f5f7;">
<tr><td align="center" style="padding:20px 10px;">

  <table role="presentation" width="860" cellpadding="0" cellspacing="0" border="0"
         style="max-width:860px; width:100%; background:#ffffff; border-radius:8px;">

    <tr>
      <td style="background:#1a2332; padding:36px 40px; font-family:{FONTE}; border-radius:8px 8px 0 0;">
        <div style="font-size:24px; font-weight:bold; color:#ffffff; padding-bottom:8px;">Relatório de Voz do Cliente</div>
        <div style="font-size:13px; color:#9aa7b8;">Análise automatizada de avaliações &middot; {data_hoje}</div>
      </td>
    </tr>

    <tr>
      <td style="padding:10px 30px; border-bottom:1px solid #eeeeee;">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
          <tr>{kpi_cells}</tr>
        </table>
      </td>
    </tr>

    <tr>
      <td style="padding:28px 40px 10px 40px; font-family:{FONTE};">
        <div style="font-size:17px; font-weight:bold; color:#2c3e50; border-left:4px solid #e67e22; padding-left:12px; margin-bottom:14px;">
          Assuntos por criticidade
        </div>
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
          <tr>
            <th align="left" style="{th}">Assunto</th>
            <th align="center" style="{th}">Reviews</th>
            <th align="center" style="{th}">Rating médio</th>
            <th align="center" style="{th}">Idade média</th>
            <th align="center" style="{th}">% negativo</th>
          </tr>
          {linhas_tabela}
        </table>
      </td>
    </tr>

    <tr>
      <td style="padding:28px 40px; font-family:{FONTE};">
        <div style="font-size:17px; font-weight:bold; color:#2c3e50; border-left:4px solid #e67e22; padding-left:12px; margin-bottom:14px;">
          Resumos executivos &mdash; principais problemas e ações
        </div>
        {blocos_resumo}
      </td>
    </tr>

    <tr>
      <td style="padding:22px 40px; border-top:1px solid #eeeeee; font-family:{FONTE}; font-size:12px; color:#95a5a6; border-radius:0 0 8px 8px;">
        Gerado automaticamente a partir de {total_reviews} avaliações usando classificação por LLM.<br>
        Projeto: review-insights-llm-databricks
      </td>
    </tr>

  </table>
</td></tr></table>
</body></html>"""

print("✅ HTML montado:", len(html), "caracteres")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizar no notebook

# COMMAND ----------

displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar no Volume

# COMMAND ----------

caminho_fixo = f"{OUTPUT_PATH}/relatorio_voc.html"
with open(caminho_fixo, "w", encoding="utf-8") as f:
    f.write(html)
print(f"✅ Relatório (nome fixo) salvo em: {caminho_fixo}")

caminho_datado = f"{OUTPUT_PATH}/relatorio_voc_{datetime.now().strftime('%Y%m%d')}.html"
with open(caminho_datado, "w", encoding="utf-8") as f:
    f.write(html)
print(f"✅ Cópia datada salva em: {caminho_datado}")

print("\nO GitHub Actions baixa 'relatorio_voc.html' automaticamente via API.")
