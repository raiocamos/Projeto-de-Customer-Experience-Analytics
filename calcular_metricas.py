import pandas as pd
from sqlalchemy import create_engine, text
import os


DATABASE_URL = "postgresql://postgres.[SEU POOLER]:[SUA SENHA]@aws-1-us-east-2.pooler.supabase.com:6543/postgres"

# Pasta de saída
PASTA_DADOS = "C:/Users/BLUE SKY INFORMATICA/Downloads/data"

print("---  INICIANDO CÁLCULO DE MÉTRICAS (VERSÃO CORRIGIDA) ---")

try:
    engine = create_engine(DATABASE_URL)
    conn = engine.connect()
    print(" Conexão OK!")
except Exception as e:
    print(f" Erro de conexão: {e}")
    exit()

def salvar_csv(df, nome_arquivo):
    caminho = os.path.join(PASTA_DADOS, nome_arquivo)
    if df.empty:
        print(f" AVISO: {nome_arquivo} ficou VAZIO.")
    else:
        df.to_csv(caminho, index=False, sep=';', decimal=',')
        print(f" Gerado: {nome_arquivo} ({len(df)} linhas)")

# 1. NPS POR CANAL
print("Calculando NPS por Canal...")
sql_canal = """
SELECT 
    p.canal,
    COUNT(r.id_resposta) as total_respostas,
    ROUND((SUM(CASE WHEN r.nps >= 9 THEN 1 ELSE 0 END)::numeric / COUNT(*) - 
           SUM(CASE WHEN r.nps <= 6 THEN 1 ELSE 0 END)::numeric / COUNT(*)) * 100, 2) as nps_score,
    ROUND(AVG(r.nota_satisfacao), 2) as media_csat
FROM respostas r
JOIN pesquisas p ON r.id_pesquisa = p.id_pesquisa
GROUP BY p.canal;
"""
df_canal = pd.read_sql(text(sql_canal), conn)
salvar_csv(df_canal, "metricas_nps_canal.csv")

# 2. SATISFAÇÃO POR REGIÃO
print("Calculando Satisfação por Região...")
sql_regiao = """
SELECT 
    re.regiao,
    COUNT(r.id_resposta) as total_respostas,
    ROUND(AVG(r.nota_satisfacao), 2) as nota_media_satisfacao,
    ROUND((SUM(CASE WHEN r.nps >= 9 THEN 1 ELSE 0 END)::numeric / COUNT(*) - 
           SUM(CASE WHEN r.nps <= 6 THEN 1 ELSE 0 END)::numeric / COUNT(*)) * 100, 2) as nps_score
FROM respostas r
JOIN respondentes re ON r.id_respondente = re.id_respondente
GROUP BY re.regiao;
"""
df_regiao = pd.read_sql(text(sql_regiao), conn)
salvar_csv(df_regiao, "metricas_satisfacao_regiao.csv")


print("Calculando Evolução Mensal...")

sql_mes = """
SELECT 
    TO_CHAR(p.data_pesquisa::date, 'YYYY-MM') as mes, 
    COUNT(r.id_resposta) as total_respostas,
    ROUND((SUM(CASE WHEN r.nps >= 9 THEN 1 ELSE 0 END)::numeric / COUNT(*) - 
           SUM(CASE WHEN r.nps <= 6 THEN 1 ELSE 0 END)::numeric / COUNT(*)) * 100, 2) as nps_score
FROM respostas r
JOIN pesquisas p ON r.id_pesquisa = p.id_pesquisa
GROUP BY 1 ORDER BY 1;
"""
try:
    df_mes = pd.read_sql(text(sql_mes), conn)
    salvar_csv(df_mes, "metricas_nps_mes.csv")
except Exception as e:
    print(f" Erro na query de mês: {e}")

conn.close()
print("\n FIM! Arquivos prontos para o Tableau.")
