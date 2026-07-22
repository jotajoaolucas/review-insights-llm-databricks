"""
config.py — Configuração central do projeto review-insights-llm-databricks.

Centraliza catálogo, schemas, endpoints de modelo, parâmetros e a lista fechada
de assuntos, para que os demais módulos não repitam strings mágicas.
"""

# ---------------------------------------------------------------------------
# Unity Catalog
# ---------------------------------------------------------------------------
CATALOG = "voc_project"

BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"

VOLUME_PATH = f"/Volumes/{CATALOG}/bronze/raw_files"

# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------
KAGGLE_DATASET = "nicapotato/womens-ecommerce-clothing-reviews"
CSV_FILE = "Womens Clothing E-Commerce Reviews.csv"

# ---------------------------------------------------------------------------
# Foundation Model API (endpoints nativos do Databricks)
# ---------------------------------------------------------------------------
EMBEDDING_ENDPOINT = "databricks-gte-large-en"
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

# ---------------------------------------------------------------------------
# Parâmetros de processamento
# ---------------------------------------------------------------------------
AMOSTRA = 3000   # nº de avaliações a processar (controla custo/tempo da IA)

# ---------------------------------------------------------------------------
# Lista fechada de assuntos
# O LLM deve classificar cada avaliação em EXATAMENTE um destes valores.
# 'outro' é a rede de segurança para o que não encaixa.
# ---------------------------------------------------------------------------
ASSUNTOS_VALIDOS = [
    "caimento", "tamanho", "conforto", "qualidade", "tecido", "cor",
    "estilo", "comprimento", "preco", "transparencia", "encolhimento", "outro",
]

# ---------------------------------------------------------------------------
# Mapeamento de colunas: originais (sanitizadas) -> snake_case
# ---------------------------------------------------------------------------
RENOMEAR_COLUNAS = {
    "_c0": "review_id",
    "Clothing_ID": "clothing_id",
    "Age": "age",
    "Title": "title",
    "Review_Text": "review_text",
    "Rating": "rating",
    "Recommended_IND": "recommended",
    "Positive_Feedback_Count": "positive_feedback_count",
    "Division_Name": "division_name",
    "Department_Name": "department_name",
    "Class_Name": "class_name",
}
