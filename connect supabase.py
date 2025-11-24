import pandas as pd
from sqlalchemy import create_engine, text
import os



DATABASE_URL = "postgresql://postgres.[SEU POOLER]:[SUA SENHA]@aws-1-us-east-2.pooler.supabase.com:6543/postgres"

# Caminho do CSV
CAMINHO_CSV = "C:/Users/BLUE SKY INFORMATICA/Downloads/data/pesquisa_bruta.csv"

print("--- INICIANDO CARGA ---")

# 1. Testar Conexão
try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print(" Conexão com Supabase OK!")
except Exception as e:
    print(f" Erro de conexão: {e}")
    exit()

# 2. Ler CSV
try:
    df = pd.read_csv(CAMINHO_CSV)
    print(f" CSV lido: {len(df)} linhas encontradas.")
except Exception:
    print(" Erro: CSV não encontrado ou vazio.")
    exit()


print("--- Limpando tabelas antigas (Isso evita o erro de dependência) ---")
try:
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS respostas CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS respondentes CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS pesquisas CASCADE"))
        conn.commit()
    print(" Tabelas antigas removidas com sucesso!")
except Exception as e:
    print(f" Aviso na limpeza: {e}")


print("--- Preparando IDs no Python ---")

# Prepara Respondentes
respondentes = df[["idade", "genero", "regiao"]].drop_duplicates().reset_index(drop=True)
respondentes["id_respondente"] = respondentes.index + 1

# Prepara Pesquisas
pesquisas = df[["data_pesquisa", "canal"]].drop_duplicates().reset_index(drop=True)
pesquisas["id_pesquisa"] = pesquisas.index + 1

print("-> Cruzando dados...")
df_final = df.merge(respondentes, on=["idade", "genero", "regiao"], how="left")
df_final = df_final.merge(pesquisas, on=["data_pesquisa", "canal"], how="left")

respostas_upload = df_final[[
    "id_resposta", "id_respondente", "id_pesquisa", 
    "nps", "nota_satisfacao", "recompraria", "comentario"
]]


print("--- Enviando para o Supabase... ---")

try:
    # Envia tabelas
    respondentes.to_sql('respondentes', engine, if_exists='replace', index=False, method='multi', chunksize=500)
    print(" Tabela 'respondentes' enviada!")

    pesquisas.to_sql('pesquisas', engine, if_exists='replace', index=False, method='multi', chunksize=500)
    print(" Tabela 'pesquisas' enviada!")

    respostas_upload.to_sql('respostas', engine, if_exists='replace', index=False, method='multi', chunksize=500)
    print(" Tabela 'respostas' enviada!")

    print("\n SUCESSO TOTAL! Dados carregados.")

except Exception as e:
    print(f"\n Erro no envio: {e}")
