# Review Insights com LLM — Voice of Customer no Databricks

Pipeline de engenharia de dados que transforma milhares de avaliações de clientes
em inteligência acionável, usando LLMs para classificar **assunto** e **sentimento**
de cada avaliação, e automação para distribuir um relatório executivo por email.

Construído inteiramente no **Databricks Free Edition**, com arquitetura Medallion,
Foundation Model APIs e orquestração cross-plataforma via GitHub Actions.

---

## O problema

Uma loja com milhares de avaliações de clientes não consegue ler tudo manualmente.
Quais são os temas recorrentes? Sobre o que os clientes mais reclamam? Onde focar
esforço de melhoria? Este projeto responde essas perguntas automaticamente.

## A solução

Cada avaliação é enriquecida com dois campos gerados por LLM:

- **assunto** — o tema do comentário, classificado numa lista fechada de categorias:
  caimento, tamanho, conforto, qualidade, tecido, cor, estilo, comprimento, preço,
  transparência, encolhimento (e *outro* como rede de segurança)
- **sentimento** — positivo, negativo ou neutro

O **produto** não é classificado por LLM — já vem estruturado no dataset
(`department_name`, `class_name`). Com esses campos, é possível cruzar
**assunto × sentimento × produto** e responder perguntas de negócio: "sobre o tema
*caimento*, qual a taxa de insatisfação?", "qual assunto tem o pior sentimento?",
"em qual departamento o problema de tamanho é mais grave?".

---

## Arquitetura

```
Kaggle API
   ↓
🥉 BRONZE      avaliações brutas + metadados de auditoria
   ↓
🥈 SILVER      texto limpo → embeddings (FM API) → assunto/sentimento (LLM)
   ↓
🥇 GOLD        avaliações classificadas + agregações + resumo executivo
   ↓
📊 ANALYTICS   mapa semântico 2D, matriz assunto×sentimento, ranking de dor
   ↓
📧 DISTRIBUIÇÃO  relatório HTML + envio automático por email (GitHub Actions)
```

### As camadas de IA (todas gratuitas via Foundation Model API)

| Etapa | Modelo | Função |
|---|---|---|
| Embeddings | `databricks-gte-large-en` | vetor de significado por avaliação (1024D) |
| Classificação | `databricks-meta-llama-3-3-70b-instruct` | assunto + sentimento (uma chamada, JSON) |
| Resumo executivo | `databricks-meta-llama-3-3-70b-instruct` | sintetiza problemas + sugere ações |

---

## Decisões de design

**Assunto por LLM com lista fechada, não por clusterização.** A primeira tentativa
foi descobrir os assuntos via clusterização de embeddings (K-Means). Não funcionou
bem: os clusters misturavam tipo de produto com tema, e com assunto livre o LLM
gerou mais de 120 categorias fragmentadas (`confort`/`conforto`, palavras em inglês,
tipos de peça). A solução foi restringir o assunto a uma **lista fechada** de 12
categorias canônicas diretamente no prompt. Resultado: campos limpos, consistentes
e diretamente filtráveis. Os embeddings foram mantidos e reaproveitados na
visualização (mapa semântico 2D).

**Produto vem da fonte, não do LLM.** O dataset já traz `department_name` e
`class_name` de forma estruturada — reclassificar isso com LLM seria desperdício de
chamadas. O LLM foca no que só ele resolve: interpretar o texto livre.

**Uma chamada de LLM para dois campos.** Assunto e sentimento saem do mesmo prompt,
retornados em JSON. Reduz pela metade o número de chamadas (importante nos limites
de rate do Free Edition) e mantém coerência entre os dois campos.

**Validação embutida.** O pipeline confere se os assuntos ficaram dentro da lista
permitida, se há sentimentos indefinidos (JSON malformado), e faz um sanity check
cruzando sentimento com rating (negativo deve ter nota média mais baixa).

---

## Distribuição automatizada

O Databricks Free Edition bloqueia envio direto de email (rede de saída restrita).
A solução separa **processamento** (Databricks) de **distribuição** (GitHub Actions),
com tudo iniciado de um único lugar — o Databricks:

```
Databricks gera o relatório HTML e salva no Volume
   → dispara o GitHub Actions via API (workflow_dispatch)
      → Actions baixa o relatório (Databricks Files API)
         → envia por email via Gmail (SMTP)
```

Credenciais ficam em secrets (GitHub) e em arquivos no Volume (Databricks) — nunca
no código. O token do Databricks usa o escopo `files` para permitir o download via API.

---

## Stack

Databricks Free Edition · PySpark · Delta Lake · Unity Catalog ·
Foundation Model API (embeddings + LLM) · GitHub Actions · Python (smtplib) · Kaggle API

---

## Estrutura do repositório

```
review-insights-llm-databricks/
├── notebooks/
│   ├── 01_Setup e Ingestão              # ambiente + teste da FM API + ingestão
│   ├── 02_bronze_reviews                # ingestão bruta
│   ├── 03_silver_reviews_enriched       # limpeza + embeddings + assunto/sentimento
│   ├── 04_gold_reviews_classified       # tabela principal
│   ├── 05_gold_agregacoes               # matriz assunto × sentimento, resumos
│   ├── 06_gold_resumo_executivo         # resumo executivo via LLM
│   ├── 07_gera_relatorio_executivo_html # relatório HTML
│   ├── 08_disparo_email_git             # dispara o GitHub Actions
│   └── 09_analytics_visualizacao        # visualizações
├── src/                                 # versão de produção (scripts enxutos)
│   ├── config.py                        # catálogo, endpoints, lista de assuntos
│   ├── bronze.py
│   ├── silver.py
│   └── gold.py
├── scripts/
│   ├── send_report.py                   # envio via GitHub Actions
│   └── send_report_databricks.py        # variante para execução local
└── .github/workflows/
    └── enviar_relatorio_auto.yml        # workflow de envio
```

---

## Tabelas produzidas

| Camada | Tabela | Conteúdo |
|---|---|---|
| Bronze | `reviews` | avaliações brutas + auditoria |
| Silver | `reviews_embeddings` | + vetor de embedding (1024D) |
| Silver | `reviews_enriched` | + assunto + sentimento |
| Gold | `reviews_classified` | tabela principal, pronta para filtrar |
| Gold | `sentimento_por_assunto` | matriz assunto × sentimento |
| Gold | `assunto_resumo` | volume, rating e % negativo por assunto |
| Gold | `sentimento_por_departamento` | cruzamento com o produto |
| Gold | `resumo_executivo` | bullets acionáveis por assunto crítico (LLM) |

---

## Dataset

[Women's E-Commerce Clothing Reviews](https://www.kaggle.com/datasets/nicapotato/womens-ecommerce-clothing-reviews)
— ~23.486 avaliações reais e anonimizadas, com texto, nota, idade e categoria de produto.
O pipeline processa uma amostra parametrizável (padrão 3.000) para respeitar os
limites de compute e de rate de API do Free Edition.

---

## Resultado

Um relatório executivo automatizado que identifica os assuntos mais críticos,
mede o sentimento de cada um e sugere ações — o tipo de análise de *voice of customer*
que áreas de produto e marketing usam para priorizar melhorias, gerado sem intervenção
manual e entregue por email.

---

## Observações sobre o Free Edition

Este projeto foi construído dentro das restrições do Databricks Free Edition: compute
serverless com cota diária, rede de saída restrita a domínios permitidos, e limites
de rate nas Foundation Model APIs. As decisões de arquitetura (amostragem, lista
fechada de assuntos, uma chamada de LLM para dois campos, distribuição via GitHub
Actions) refletem essas restrições — mostrando adaptação prática a um ambiente com
limitações reais.
