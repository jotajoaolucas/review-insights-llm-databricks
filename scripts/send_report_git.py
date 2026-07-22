"""
Envia o relatório de Voz do Cliente por email via Gmail.

O email vai com:
  - o relatório HTML renderizado no corpo da mensagem
  - o PDF anexado (se disponível)
  - o HTML anexado (fallback, caso o cliente de email não renderize bem)

Projetado para rodar no GitHub Actions, lendo credenciais dos secrets.

Variáveis de ambiente:
  GMAIL_USER          -> email do remetente
  GMAIL_APP_PASSWORD  -> senha de app de 16 caracteres do Gmail
  EMAIL_DESTINO       -> destinatário(s), separados por vírgula
  REPORT_PATH         -> caminho do HTML (opcional; busca em reports/ se ausente)
  PDF_PATH            -> caminho do PDF (opcional; anexa se existir)
"""

import os
import glob
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime


def encontrar_relatorio():
    caminho_env = os.environ.get("REPORT_PATH")
    if caminho_env and os.path.exists(caminho_env):
        return caminho_env

    arquivos = glob.glob("reports/relatorio_voc*.html") or glob.glob("reports/*.html")
    if not arquivos:
        print("❌ Nenhum relatório HTML encontrado em reports/")
        sys.exit(1)

    mais_recente = max(arquivos, key=os.path.getmtime)
    print(f"📄 Relatório encontrado: {mais_recente}")
    return mais_recente


def montar_email(remetente, destinatarios, caminho_html, caminho_pdf):
    with open(caminho_html, "r", encoding="utf-8") as f:
        conteudo_html = f.read()

    data_hoje = datetime.now().strftime("%d/%m/%Y")

    msg = MIMEMultipart("mixed")
    msg["From"] = remetente
    msg["To"] = destinatarios
    msg["Subject"] = f"Relatório de Voz do Cliente — {data_hoje}"

    # corpo: versão texto + versão HTML (o cliente escolhe a melhor que suporta)
    corpo = MIMEMultipart("alternative")
    texto_simples = (
        "Relatório de Voz do Cliente gerado automaticamente.\n\n"
        "O conteúdo completo está no corpo desta mensagem (HTML) e nos anexos "
        "(PDF e HTML). Se você está lendo isto, seu cliente de email não exibiu "
        "a versão formatada — abra o PDF anexo."
    )
    corpo.attach(MIMEText(texto_simples, "plain", "utf-8"))
    corpo.attach(MIMEText(conteudo_html, "html", "utf-8"))
    msg.attach(corpo)

    # anexo 1: PDF (se existir)
    if caminho_pdf and os.path.exists(caminho_pdf):
        with open(caminho_pdf, "rb") as f:
            anexo_pdf = MIMEApplication(f.read(), _subtype="pdf")
        anexo_pdf.add_header("Content-Disposition", "attachment",
                             filename=f"relatorio_voc_{datetime.now().strftime('%Y%m%d')}.pdf")
        msg.attach(anexo_pdf)
        print(f"📎 PDF anexado: {caminho_pdf}")
    else:
        print("ℹ️  PDF não encontrado — enviando apenas HTML")

    # anexo 2: HTML (fallback)
    anexo_html = MIMEApplication(conteudo_html.encode("utf-8"), _subtype="html")
    anexo_html.add_header("Content-Disposition", "attachment",
                          filename=os.path.basename(caminho_html))
    msg.attach(anexo_html)

    return msg


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

    caminho_html = encontrar_relatorio()
    caminho_pdf = os.environ.get("PDF_PATH", "reports/relatorio_voc.pdf")

    msg = montar_email(remetente, destinatarios, caminho_html, caminho_pdf)

    print(f"📤 Conectando ao Gmail como {remetente}...")
    with smtplib.SMTP("smtp.gmail.com", 587) as servidor:
        servidor.starttls()
        servidor.login(remetente, senha)
        servidor.sendmail(remetente, destinatarios.split(","), msg.as_string())

    print(f"✅ Email enviado para {destinatarios}")


if __name__ == "__main__":
    main()
