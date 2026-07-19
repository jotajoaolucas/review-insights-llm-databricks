"""
Envia o relatório HTML de Voz do Cliente por email via Gmail.

Projetado para rodar no GitHub Actions, lendo credenciais dos secrets
(nunca hardcoded). Também pode ser executado localmente para teste,
desde que as variáveis de ambiente estejam definidas.

Variáveis de ambiente esperadas:
  GMAIL_USER          -> email do remetente (ex: voce@gmail.com)
  GMAIL_APP_PASSWORD  -> senha de app de 16 caracteres do Gmail
  EMAIL_DESTINO       -> email(s) do destinatário (separados por vírgula)
  REPORT_PATH         -> caminho do arquivo HTML (opcional; default abaixo)
"""

import os
import glob
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime


import os

# lê a senha do Gmail do arquivo no Volume
with open("/Volumes/workspace/default/tokens/email_token.txt") as f:
    senha_gmail = f.read().strip()

# define as variáveis de ambiente que o send_report.py espera
os.environ["GMAIL_USER"] = "jotajoaolucas@gmail.com"          # seu email remetente
os.environ["GMAIL_APP_PASSWORD"] = senha_gmail            # lida do arquivo
os.environ["EMAIL_DESTINO"] = "jotajoaolucas@gmail.com" # quem recebe
os.environ["REPORT_PATH"] = "/Volumes/voc_project/bronze/raw_files/relatorio_voc.html"


def encontrar_relatorio():
    """Localiza o relatório HTML mais recente na pasta reports/."""
    caminho_env = os.environ.get("REPORT_PATH")
    if caminho_env and os.path.exists(caminho_env):
        return caminho_env

    # procura o HTML mais recente em reports/
    arquivos = glob.glob("reports/relatorio_voc_*.html")
    if not arquivos:
        arquivos = glob.glob("reports/*.html")
    if not arquivos:
        print("❌ Nenhum relatório HTML encontrado em reports/")
        sys.exit(1)

    mais_recente = max(arquivos, key=os.path.getmtime)
    print(f"📄 Relatório encontrado: {mais_recente}")
    return mais_recente


def enviar_email(remetente, senha, destinatarios, caminho_html):
    with open(caminho_html, "r", encoding="utf-8") as f:
        conteudo_html = f.read()

    data_hoje = datetime.now().strftime("%d/%m/%Y")

    msg = MIMEMultipart("mixed")
    msg["From"] = remetente
    msg["To"] = destinatarios
    msg["Subject"] = f"Relatório de Voz do Cliente — {data_hoje}"

    # corpo do email = o próprio HTML (renderiza inline no cliente de email)
    corpo = MIMEMultipart("alternative")
    texto_simples = ("Segue em anexo o Relatório de Voz do Cliente gerado automaticamente. "
                     "Se o email não exibir o conteúdo, abra o anexo HTML.")
    corpo.attach(MIMEText(texto_simples, "plain", "utf-8"))
    corpo.attach(MIMEText(conteudo_html, "html", "utf-8"))
    msg.attach(corpo)

    # também anexa o HTML como arquivo (garantia)
    anexo = MIMEApplication(conteudo_html.encode("utf-8"), _subtype="html")
    anexo.add_header("Content-Disposition", "attachment",
                     filename=os.path.basename(caminho_html))
    msg.attach(anexo)

    # envio via SMTP do Gmail (TLS)
    print(f"📤 Conectando ao Gmail como {remetente}...")
    with smtplib.SMTP("smtp.gmail.com", 587) as servidor:
        servidor.starttls()
        servidor.login(remetente, senha)
        servidor.sendmail(remetente, destinatarios.split(","), msg.as_string())

    print(f"✅ Email enviado para {destinatarios}")


def main():
    remetente = os.environ.get("GMAIL_USER")
    senha = os.environ.get("GMAIL_APP_PASSWORD")
    destinatarios = os.environ.get("EMAIL_DESTINO")

    faltando = [n for n, v in [("GMAIL_USER", remetente),
                                ("GMAIL_APP_PASSWORD", senha),
                                ("EMAIL_DESTINO", destinatarios)] if not v]
    if faltando:
        print(f"❌ Variáveis de ambiente faltando: {', '.join(faltando)}")
        sys.exit(1)

    caminho = encontrar_relatorio()
    enviar_email(remetente, senha, destinatarios, caminho)


if __name__ == "__main__":
    main()
