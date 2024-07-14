import pandas as pd

def calcular_iqar(concentracao, faixas):
    for (c_ini, c_fim, i_ini, i_fim) in faixas:
        if c_ini <= concentracao <= c_fim:
            return ((i_fim - i_ini) / (c_fim - c_ini)) * (concentracao - c_ini) + i_ini
    return None

# faixas de índice para cada poluente
faixas_poluentes = {
    'pm10': [(0, 50, 0, 40), (51, 100, 41, 80), (101, 150, 81, 120), (151, 250, 121, 200), (251, 600, 201, 400)],
    'pm2_5': [(0, 25, 0, 40), (26, 50, 41, 80), (51, 75, 81, 120), (76, 125, 121, 200), (126, 300, 201, 400)],
    'so2': [(0, 20, 0, 40), (21, 40, 41, 80), (41, 365, 81, 120), (366, 800, 121, 200), (801, 2620, 201, 400)],
    'no2': [(0, 200, 0, 40), (201, 240, 41, 80), (241, 320, 81, 120), (321, 1130, 121, 200), (1131, 3750, 201, 400)],
    'o3': [(0, 100, 0, 40), (101, 130, 41, 80), (131, 160, 81, 120), (161, 200, 121, 200), (201, 800, 201, 400)],
    'co': [(0, 9, 0, 40), (10, 11, 41, 80), (12, 13, 81, 120), (14, 15, 121, 200), (16, 50, 201, 400)],
}

url = './Qualidade_do_ar_Dados_horarios.csv'
dados_brutos = pd.read_csv(url, delimiter=',', skipinitialspace=True, na_values=['', ' '])
dados_brutos['data'] = pd.to_datetime(dados_brutos['data'])

df = dados_brutos.copy()

# calcula o IQar para cada poluente
for poluente in faixas_poluentes.keys():
  df[poluente].fillna(0, inplace=True)
  df[f'IQar_{poluente}'] = df[poluente].apply(lambda x: calcular_iqar(x, faixas_poluentes[poluente]))

agregacao = {}
max = [f'IQar_{poluente}' for poluente in faixas_poluentes.keys()]
media = [f'{poluente}' for poluente in faixas_poluentes.keys()] + ['chuva',	'pres',	'rs',	'temp',	'ur',	'dir_vento', 'vel_vento']
for i in df.columns:
  if i in max:
    agregacao[i] = 'max'
  elif i in media:
    agregacao[i] = 'mean'

df = df.groupby([df['data'].dt.date, 'estação', 'lat',	'lon',	'x_utm_sirgas2000',	'y_utm_sirgas2000']).agg(agregacao).reset_index()
df['data'] = pd.to_datetime(df['data'])

def classificar_valor(valor, faixa):
    for i, (_, _, i_ini, i_fim) in enumerate(faixa):
        if i_ini <= valor <= i_fim:
            return i
    return None  # Valor fora das faixas

def classificar_texto(valor):
    classificacoes = ['boa', 'moderada', 'ruim', 'muito ruim', 'pessima']
    return classificacoes[valor]

for poluente in faixas_poluentes.keys():
  df[f'class_{poluente}'] = df[f'IQar_{poluente}'].apply(lambda x: classificar_valor(x, faixas_poluentes[poluente]))

df['classificacao'] = df[['class_pm10', 'class_pm2_5', 'class_so2', 'class_no2', 'class_o3', 'class_co']].max(axis=1)
df['classificacao'] = df['classificacao'].apply(lambda x: classificar_texto(int(x)))

# dimensão DTempo
d_tempo = df[['data']].copy()
d_tempo['dia'] = d_tempo['data'].dt.day
d_tempo['mes'] = d_tempo['data'].dt.month
d_tempo['ano'] = d_tempo['data'].dt.year
d_tempo = d_tempo.drop_duplicates().reset_index(drop=True)
d_tempo['pk_tempo'] = d_tempo.index + 1
d_tempo.drop(columns=['data'], inplace=True)

estacoes = {
'BG': ['Bangu', 'Zona Oeste'],
'SC': ['São Cristóvão', 'Zona Central'],
'IR': ['Irajá', 'Zona Norte'],
'CA': ['Centro', 'Zona Central'],
'AV': ['Copacabana', 'Zona Sul'],
'SP': ['Tijuca', 'Zona Norte'],
'CG': ['Campo Grande', 'Zona Oeste'],
'PG': ['Pedra de Guaratiba', 'Zona Oeste'],
}

def mapear_estacao(estacao):
  if estacao in estacoes:
      return estacoes[estacao][0], estacoes[estacao][1]
  else:
      return 'Desconhecido', 'Desconhecido'
  
# dimensão DEstacao
d_estacao = df[['estação', 'lat', 'lon', 'x_utm_sirgas2000', 'y_utm_sirgas2000']].drop_duplicates().reset_index(drop=True)
d_estacao['bairro'], d_estacao['zona'] = zip(*d_estacao['estação'].map(mapear_estacao))
d_estacao['pk_estacao'] = d_estacao.index + 1

# dimensão DCondicaoClimatica
d_condicao_climatica = df[['dir_vento', 'vel_vento', 'rs', 'pres', 'chuva', 'ur', 'temp']].drop_duplicates().reset_index(drop=True)
d_condicao_climatica['pk_condicao_climatica'] = d_condicao_climatica.index + 1

# dimensão DClassificacao
d_classificacao = df[['classificacao']].drop_duplicates().reset_index(drop=True)
d_classificacao['pk_classificacao'] = d_classificacao.index + 1

# fato FQualidadeAr
f_qualidade_ar = df.merge(d_tempo, on='data').merge(d_estacao, on='estação').merge(d_condicao_climatica, on=['dir_vento', 'vel_vento', 'rs', 'pres', 'chuva', 'ur', 'temp']).merge(d_classificacao, on=['classificacao'])
f_qualidade_ar = f_qualidade_ar[['pk_tempo', 'pk_estacao', 'pk_condicao_climatica', 'pk_classificacao'] + [poluente for poluente in faixas_poluentes.keys()] + [f'IQar_{poluente}' for poluente in faixas_poluentes.keys()]]

f_qualidade_ar = f_qualidade_ar.rename(columns={
    'pk_tempo': 'fk_tempo',
    'pk_estacao': 'fk_estacao',
    'pk_condicao_climatica': 'fk_condicao_climatica',
    'pk_classificacao': 'fk_classificacao',
    'pm10': 'concentracao_mp10',
    'pm2_5': 'concentracao_mp2_5',
    'so2': 'concentracao_so2',
    'no2': 'concentracao_no2',
    'o3': 'concentracao_o3',
    'co': 'concentracao_co',
})

# Criar arquivos csv
d_tempo.to_csv('d_tempo.csv', index=False)
d_estacao.to_csv('d_estacao.csv', index=False)
d_condicao_climatica.to_csv('d_condicao_climatica.csv', index=False)
d_classificacao.to_csv('d_classificacao.csv', index=False)
f_qualidade_ar.to_csv('f_qualidade_ar.csv', index=False)