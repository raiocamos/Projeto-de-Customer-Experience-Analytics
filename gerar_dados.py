import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os


CAMINHO_ARQUIVO = "CAMINHO DO ARQUIVO"

print(f"Tentando salvar em: {CAMINHO_ARQUIVO}")

# GERAÇÃO DOS DADOS
np.random.seed(42)
n_respostas = 1000
canais = ["Site", "App", "Loja Física", "Call Center"]
regioes = ["Sudeste", "Sul", "Nordeste", "Centro-Oeste", "Norte"]
generos = ["M", "F", "Outro"]
prob_canais = [0.4, 0.3, 0.2, 0.1]

data_inicio = datetime(2024, 1, 1)
datas = [data_inicio + timedelta(hours=np.random.randint(0, 8760)) for _ in range(n_respostas)]

df = pd.DataFrame({
    "id_resposta": range(1, n_respostas + 1),
    "data_pesquisa": datas,
    "canal": np.random.choice(canais, size=n_respostas, p=prob_canais),
    "nps": np.random.randint(0, 11, size=n_respostas),
    "nota_satisfacao": np.random.randint(1, 6, size=n_respostas),
    "regiao": np.random.choice(regioes, size=n_respostas),
    "idade": np.random.randint(18, 71, size=n_respostas),
    "genero": np.random.choice(generos, size=n_respostas),
    "recompraria": np.random.choice([True, False], size=n_respostas, p=[0.7, 0.3]),
    "comentario": [f"Comentário simulado {i}" for i in range(1, n_respostas + 1)]
})

# SALVAR
try:
    df.to_csv(CAMINHO_ARQUIVO, index=False, encoding="utf-8")
    print("\n SUCESSO TOTAL!")
    print(f"O arquivo foi criado aqui: {CAMINHO_ARQUIVO}")
except OSError as e:
    print("\n AINDA DEU ERRO:")
    print(e)
    print("Verifique se você criou a pasta 'data' dentro de Downloads!")
