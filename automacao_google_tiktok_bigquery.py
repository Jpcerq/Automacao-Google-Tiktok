import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import tkinter as tk
from tkinter import filedialog, messagebox
import io

# ── Configuração do BigQuery ──────────────────────────────────────────────────

KEY_PATH   = r"C:\Users\joaop\OneDrive\Documentos\Ideia3\Testes\AutomaçãoBaseManualGoogleTiktok\ChaveBigquery\basededados-428619-bd416392eb22.json"

PROJECT_ID    = "basededados-428619"
DATASET       = "dados_compartilhados"
TABELA_GOOGLE = f"{PROJECT_ID}.{DATASET}.base_manual_google"
TABELA_TIKTOK = f"{PROJECT_ID}.{DATASET}.base_manual_tiktok"

# ── Conexão com BigQuery ──────────────────────────────────────────────────────

credentials = service_account.Credentials.from_service_account_file(
    KEY_PATH,
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
print("Conexão com BigQuery estabelecida com sucesso!")

# ── Seleção de arquivos via interface gráfica ─────────────────────────────────

root = tk.Tk()
root.withdraw()  # Oculta a janela principal do tkinter

print("\nSelecione o arquivo do TikTok...")
caminho_tiktok = filedialog.askopenfilename(
    title="Selecione o arquivo do TikTok",
    filetypes=[("Excel files", "*.xlsx")]
)

print("Selecione o arquivo do Google...")
caminho_google = filedialog.askopenfilename(
    title="Selecione o arquivo do Google",
    filetypes=[("Excel files", "*.xlsx")]
)

if not caminho_tiktok or not caminho_google:
    messagebox.showerror("Erro", "Nenhum arquivo selecionado. Encerrando.")
    raise SystemExit("Arquivos não selecionados.")

print(f"\nTikTok: {caminho_tiktok}")
print(f"Google: {caminho_google}")

# ── Tratamento — TikTok ───────────────────────────────────────────────────────

df1 = pd.read_excel(caminho_tiktok)

df1 = df1.iloc[:-1]

mapeamento_colunas = {
    "Nome do anúncio": "Ad Name",
    "Por dia": "By Day",
    "Nome da campanha": "Campaign Name",
    "Pedidos feitos (Off-line)": "Orders Placed Offline",
    "Valor dos pedidos feitos (Off-line)": "Orders Placed Value Offline"
}

df1 = df1.rename(columns=mapeamento_colunas)
df1 = df1.drop(columns=["Moeda"], errors="ignore")

if "By Day" in df1.columns:
    df1["By Day"] = pd.to_datetime(df1["By Day"]).dt.date

print("\nTikTok — shape:", df1.shape)
print(df1.dtypes)

# ── Tratamento — Google ───────────────────────────────────────────────────────

df2 = pd.read_excel(caminho_google, skiprows=2)

df2 = df2.rename(columns={
    "Todas as conv.": "Todas as conv",
    "Valor de todas as conv.": "Valor de todas as conv"
})

df2["Dia"] = pd.to_datetime(df2["Dia"]).dt.date

df2["Valor de todas as conv"] = (
    df2["Valor de todas as conv"]
    .astype(str)
    .str.replace(",", ".", regex=False)
    .astype(float)
)

print("\nGoogle — shape:", df2.shape)
print(df2.dtypes)

# ── Carga no BigQuery (WRITE_TRUNCATE) ────────────────────────────────────────

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
)

# Carrega TikTok
print("\nCarregando TikTok no BigQuery...")
job1 = client.load_table_from_dataframe(df1, TABELA_TIKTOK, job_config=job_config)
job1.result()
print(f"TikTok carregado: {client.get_table(TABELA_TIKTOK).num_rows} linhas em {TABELA_TIKTOK}")

# Carrega Google
print("Carregando Google no BigQuery...")
job2 = client.load_table_from_dataframe(df2, TABELA_GOOGLE, job_config=job_config)
job2.result()
print(f"Google carregado: {client.get_table(TABELA_GOOGLE).num_rows} linhas em {TABELA_GOOGLE}")

print("\nProcessamento concluído com sucesso!")
messagebox.showinfo("Sucesso", "Dados carregados no BigQuery com sucesso!")
