# Importando bibliotecas
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import datetime, timedelta


# Importando arquivos
df = pd.read_csv(r'\\192.168.6.72\AcosUniao\INTRANET\Alex\Notebooks\Pedidos_para_producao\MTC246.CSV',
                sep=';', encoding='latin1', error_bad_lines=False)

estoque = pd.read_csv(r'\\192.168.6.72\AcosUniao\INTRANET\Alex\Notebooks\Pedidos_para_producao\MTC238.CSV',
                sep=';', encoding='latin1')

# Arquivo de programação de perfil
prog_marafon_02 = pd.read_excel(r'\\192.168.6.72\AcosUniao\INTRANET\PCP\PROGRAMAÇÃO DE PERFIL.xlsx', sheet_name='Marafon 02')
prog_marafon_200 = pd.read_excel(r'\\192.168.6.72\AcosUniao\INTRANET\PCP\PROGRAMAÇÃO DE PERFIL.xlsx', sheet_name='Marafon 200')
prog_zikeli = pd.read_excel(r'\\192.168.6.72\AcosUniao\INTRANET\PCP\PROGRAMAÇÃO DE PERFIL.xlsx', sheet_name='ZIKELI')

# Arquivos de programação de chapa
prog_gcr = pd.read_excel(r'\\192.168.6.72\AcosUniao\INTRANET\PCP\PROGRAMAÇÃO BUENO.xlsx', sheet_name='GCR 6,30mm')
prog_marafon = pd.read_excel(r'\\192.168.6.72\AcosUniao\INTRANET\PCP\PROGRAMAÇÃO BUENO.xlsx', sheet_name='TRANVERSAL MARAFON')
prog_meia = pd.read_excel(r'\\192.168.6.72\AcosUniao\INTRANET\PCP\PROGRAMAÇÃO BUENO.xlsx', sheet_name='TRANVERSAL 1 2" ')

# selecionando, renomenado e formantando colunas do dataframe estoque
estoque = estoque[['Descricao', 'Quantidade atual']]
estoque.columns = ['Item', 'Estoque']

estoque.Estoque = estoque.Estoque.str.replace('.','')
estoque.Estoque = estoque.Estoque.astype(float)/1000

estoque= estoque.drop_duplicates(subset='Estoque', keep='last')

# Excluindo linhas em branco pela coluna "Item"
df.dropna(subset=['Item'], inplace=True)

# Selecionando as colunas que vamos utilizar do DataFrame principal
df = df[['Emissao', 'Dt. Aprovacao', 'Pedido', 'Cod. Cliente', 'Razao Social Cliente',
         'Razao Social Vendedor', 'SKU', 'Descricao Material', 'Qtd. Em Aberto', 'Qtde.Pecas'  
]]

# Renomeando colunas
rename = {'Dt. Aprovacao': 'Aprovacao',
            'Cod. Cliente': 'Cod_cliente',
            'Razao Social Cliente': 'Cliente',
            'Razao Social Vendedor': 'Vendedor',
            'SKU': 'Cod_produto',
            'Descricao Material': 'Item',
            'Qtd. Em Aberto': 'Qtd_aberto',
            'Qtde.Pecas': 'Qtd_pecas'
            }

df.rename(columns=rename, inplace=True)

# Formandando colunas
df.Emissao = pd.to_datetime(df.Emissao, format = '%d/%m/%Y').dt.date
df.Qtd_aberto = df.Qtd_aberto.str.replace('.','').astype(float).round(2)/100
df.Qtd_pecas = df.Qtd_pecas.astype(float).round(0)

def aprovacao(data_aprovacao):
    if data_aprovacao != '/  /':
        return 'Aprovado'
    else:
        return 'Ag.Aprovacao/Reprovado'
    
df['Item'] = df['Item'].str.replace('^0', '', regex=True)

df['Aprovacao'] = df['Aprovacao'].apply(aprovacao)

# Criando coluna com quantidade de dias do pedido em tela
hoje = datetime.today().date()

df['Dias_em_tela'] = (hoje - df['Emissao']).dt.days
df = df.sort_values(by='Pedido', ascending=True)
df.Pedido = df.Pedido.str.replace('.','').astype(int)

# Totalizando vendas por item
df['Em_aberto_acumulado'] = df.groupby('Item')['Qtd_aberto'].cumsum()
df = df.reset_index()
df.drop(columns='index', inplace=True)

# Agrupando DataFrame de pedidos com estoque
df = pd.merge(df, estoque, on='Item', how='left')
df['Estoque'].fillna(0, inplace=True)
df['Saldo'] = df['Estoque'] - df['Em_aberto_acumulado']

# Definindo se o pedido está pronto
itens_estoque = ['CH', 'SL', 'AP']

def atribuir_situacao(df):
    for i in range(len(df)):
        saldo = df.at[i, 'Saldo']
        item = df.at[i, 'Item']

        if saldo >= 0:
            df.at[i, 'Situacao'] = 'Pronto'
        elif item.startswith(tuple(itens_estoque)):
            df.at[i, 'Situacao'] = 'Pronto'
        else:
            df.at[i, 'Situacao'] = 'Produzir'

atribuir_situacao(df)

df['Item'] = df['Item'].str.replace('.',',')

# Criando DataFrame de pedidos prontos
df_pronto = df[df['Situacao'] == 'Pronto']
df_pronto = df_pronto.groupby(['Pedido', 'Cliente'])['Qtd_aberto'].sum().reset_index()

# Criando DataFrame de pedidos em a produzir

df_produzir = df[df['Situacao'] == 'Produzir']
df_produzir = df_produzir.groupby(['Pedido', 'Cliente'])['Qtd_aberto'].sum().reset_index()

df_pedidos = df[['Pedido', 'Emissao', 'Aprovacao', 'Cliente']]
df_pedidos = pd.merge(df_pedidos, df_pronto, on='Pedido', how='left')
df_pedidos['Qtd_aberto'].fillna(0, inplace=True)
df_pedidos = df_pedidos[['Pedido', 'Emissao', 'Aprovacao', 'Cliente_x', 'Qtd_aberto']]
df_pedidos.columns = ['Pedido', 'Emissao', 'Aprovacao', 'Cliente', 'Pronto']

df_pedidos = pd.merge(df_pedidos, df_produzir, on='Pedido', how='left')
df_pedidos['Qtd_aberto'].fillna(0, inplace=True)
df_pedidos = df_pedidos[['Pedido', 'Emissao', 'Aprovacao', 'Cliente_x', 'Pronto', 'Qtd_aberto']]
df_pedidos.columns = ['Pedido', 'Emissao', 'Aprovacao', 'Cliente', 'Pronto', 'A_produzir']


# Incluindo colunas com percentual de pedido pronto
df_pedidos['Percentual_pronto'] = ((df_pedidos['Pronto'] / (df_pedidos['Pronto'] + df_pedidos['A_produzir']))*100).round(2)
df_pedidos = df_pedidos.drop_duplicates(subset='Pedido', keep='first')

# Trabalhando com DataFrame de Programação de perfil
colunas = ['Data', 'DESCRIÇÃO', 'Produzir']
prog_marafon_02 = prog_marafon_02[colunas]
prog_marafon_200 = prog_marafon_200[colunas]
prog_zikeli = prog_zikeli[colunas]

prog_marafon_02['Maquina'] = 'Marafon 02'
prog_marafon_200['Maquina'] = 'Marafon 200'
prog_zikeli['Maquina'] = 'Zikeli'

prog_perfil = pd.concat([prog_marafon_02, prog_marafon_200, prog_zikeli])

# Trabalando com DataFrame de Programação de chapa
prog_gcr['Maquina'] = 'GCR'
prog_marafon['Maquina'] = 'Marafon 1/8"'
prog_meia['Maquina'] = 'Transvessal 1/2"'

prog_chapa = pd.concat([prog_gcr, prog_marafon, prog_meia])
prog_chapa

# Separando Dataframes
df_perfil = df[df['Item'].str.startswith('U')]
df_chapa = df[df['Item'].str.startswith('CF')]

filtro_perfil = df['Item'].isin(df_perfil['Item'])
filtro_chapa = df['Item'].isin(df_chapa['Item'])

df = df[~(filtro_perfil | filtro_chapa)]

df_perfil_produzir = df_perfil[df_perfil['Situacao'] == 'Produzir']
df_perfil_pronto = df_perfil[df_perfil['Situacao'] != 'Produzir']

# Definindo Prazo para consulta na programação
data_hoje = datetime.now().date()
dia = 5
data_selecao = data_hoje - timedelta(days=dia)

data_selecao = pd.to_datetime(data_selecao)

prog_perfil['Data'] = pd.to_datetime(prog_perfil['Data'])  
prog_perfil = prog_perfil[prog_perfil['Data'] > data_selecao]

# Incluindo datas de perfil
prog_perfil = prog_perfil.groupby('DESCRIÇÃO')[['Data', 'Maquina']].first().reset_index()
df_perfil_produzir = pd.merge(df_perfil_produzir, prog_perfil, left_on='Item', right_on='DESCRIÇÃO', how='left')
df_perfil_produzir.drop(columns=['DESCRIÇÃO'], inplace=True)

df_perfil_pronto = pd.concat([df_perfil_pronto,df_perfil_produzir])

# Incluindo datas de chapa
df_chapa_pronto =  df_chapa[df_chapa['Situacao'] != 'Produzir']
df_chapa_produzir = df_chapa[df_chapa['Situacao'] == 'Produzir']

df_chapa_produzir.rename(columns={'Item': 'Descricao Material'}, inplace=True)

df_chapa_produzir = pd.merge(df_chapa_produzir, prog_chapa, on=['Pedido', 'Descricao Material'], how='left')

df_chapa_produzir.rename(columns={'DATA CORTE': 'Data',
                                  'Descricao Material':'Item',
                                  'Emissao_x': 'Emissao'}, inplace=True)

df_chapa_pronto = pd.concat([df_chapa_pronto, df_chapa_produzir])
df_chapa_pronto = df_chapa_pronto[['Emissao', 'Aprovacao', 'Pedido', 'Cod_cliente', 'Cliente', 'Vendedor', 'Cod_produto',
                                   'Item', 'Qtd_aberto', 'Qtd_pecas', 'Dias_em_tela', 'Em_aberto_acumulado',
                                   'Estoque', 'Saldo', 'Situacao', 'Data', 'Maquina']]

# agrupando DataFrames
df_chapa_pronto = pd.concat([df_chapa_pronto, df_chapa_produzir])

df = pd.concat([df, df_chapa_pronto, df_perfil_pronto])
df = df.fillna('')

df.sort_values(by='Pedido', inplace=True)

df = df.drop_duplicates(subset=['Pedido', 'Item', 'Qtd_aberto'], keep='first')

data_producao = df.groupby('Pedido')['Data'].max().reset_index()

df_pedidos = pd.merge(df_pedidos,data_producao, on='Pedido', how='left' )
df_pedidos.rename(columns={'Data': 'Data de Produção'}, inplace=True)

df_pedidos.loc[df_pedidos['Percentual_pronto'] == 1, 'Data de Produção'] = 'Pronto'

df_pedidos['Percentual_pronto'] = df_pedidos['Percentual_pronto'].astype(str) + ' %'

df = df[['Emissao', 'Aprovacao', 'Pedido', 'Cod_cliente', 'Cliente', 'Vendedor', 'Cod_produto', 'Item', 'Qtd_aberto',
        'Qtd_pecas', 'Dias_em_tela', 'Em_aberto_acumulado', 'Estoque', 'Saldo', 'Situacao', 'Data', 'Maquina']]

# Formatando datas
df_pedidos['Data de Produção'] = pd.to_datetime(df_pedidos['Data de Produção'], format='%Y-%m-%d')
df_pedidos['Emissao'] = pd.to_datetime(df_pedidos['Emissao'], format='%Y-%m-%d')

df['Emissao'] = pd.to_datetime(df['Emissao'], format='%Y-%m-%d')
df['Data'] = df['Data'].dt.strftime('%Y-%m-%d')


# Salvando Arquivo em Excel
with pd.ExcelWriter(fr'\\192.168.6.72\AcosUniao\INTRANET\Alex\Notebooks\Pedidos_para_producao\Pedidos_Prontos.xlsx', engine='xlsxwriter') as writer:

    df_pedidos.to_excel(writer, index=False, sheet_name='Por Pedido')
    
    df.to_excel(writer, index=False, sheet_name='Detalhado por item')

print('\n\n')
print('*'*50)
print('Arquivo gerado com sucesso!')
print('Pressione Enter para fechar...')
print('*'*50)
print('\n\n')
input()


