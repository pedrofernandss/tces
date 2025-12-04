import pandas as pd

def ler_dados(url: str) -> pd.DataFrame:
    tces_dataframe = pd.read_parquet(url)

    return tces_dataframe

def contar_convenios_por_ministerio(dataframe: pd.DataFrame) -> pd.DataFrame:
    contagem_convenios_por_ministerio = dataframe.groupby(
    ['ministerio', 'ano_referencia']).size().reset_index(name='quantidade_convenios')

    return contagem_convenios_por_ministerio

def contar_convenios_por_regiao(dataframe: pd.DataFrame) -> pd.DataFrame:

    regioes = {
        'AC': 'Norte', 'AP': 'Norte', 'AM': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
        'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste', 'PB': 'Nordeste', 'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
        'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste',
        'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
        'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
    }

    dataframe['regiao'] = dataframe['unidade_federativa'].map(regioes)

    convenios_pivot_regiao = pd.crosstab(
        index=[dataframe['ministerio'],
        dataframe['ano_referencia']], 
        columns=dataframe['regiao']
    )

    convenios_pivot_regiao.columns = [f'qntd_convenios_{r.lower().replace("-", "_")}' for r in convenios_pivot_regiao.columns]

    return convenios_pivot_regiao

def contar_convenios_por_situacao(dataframe: pd.DataFrame) -> pd.DataFrame:
        
    convenios_pivot_situacao = pd.crosstab(
        index=[dataframe['ministerio'], dataframe['ano_referencia']],
        columns=dataframe['situacao_convenio']
    )

    convenios_pivot_situacao.columns = [f'qtnd_convenios_{m.lower().replace("Ã", "A").replace("Ç", "C").replace(" ", "_")}'
        for m in convenios_pivot_situacao.columns
    ]

    return convenios_pivot_situacao

def contar_convenios_por_alinhamento(dataframe: pd.DataFrame) -> pd.DataFrame:

    convenios_pivot_alinhamento = pd.crosstab(
        index=[dataframe['ministerio'],dataframe['ano_referencia']],
        columns=dataframe['alinhamento_gov']
    )

    mapa_alinhamento = {0: 'nao_alinhado', 1: 'alinhado'}
    convenios_pivot_alinhamento.columns = [f'qntd_convenios_{mapa_alinhamento.get(col, col)}' for col in convenios_pivot_alinhamento.columns]

    return convenios_pivot_alinhamento

def somar_valor_convenios(dataframe: pd.DataFrame) -> pd.DataFrame: 
    
    df_valor_convenios_somado = dataframe.groupby(['ministerio', 'ano_referencia'])[
        'valor_convenio_deflacionado'].sum().reset_index(name='valor_total_convenios')

    
    df_valor_convenios_somado['valor_total_convenios'] = df_valor_convenios_somado['valor_total_convenios'].round(
        2)
    
    return df_valor_convenios_somado

def agregar_base_convenios(url: str) -> pd.DataFrame:
    df_convenios = ler_dados(url)

    df_conv_valores_somado = somar_valor_convenios(df_convenios)
    df_conv_agg_ministerios = contar_convenios_por_ministerio(df_convenios)
    df_conv_agg_regiao = contar_convenios_por_regiao(df_convenios)
    df_conv_agg_situacao = contar_convenios_por_situacao(df_convenios)
    df_conv_agg_alinhamento = contar_convenios_por_alinhamento(df_convenios)

    tces_final = pd.concat([df_conv_agg_ministerios, df_conv_agg_situacao, df_conv_agg_regiao, df_conv_agg_alinhamento, df_conv_valores_somado], axis=1)
    tces_final = tces_final.reset_index()

    return tces_final

if __name__ == "__main__":
    tces_database_url = './database/clean/convenios_clean.parquet'
    local_salvamento = './database/aggregated/convenios_aggregated.parquet'

    tces_agregado = agregar_base_convenios(tces_database_url)
    tces_agregado.to_parquet(local_salvamento, index=False)

    print(f"Base de dados agregada!")