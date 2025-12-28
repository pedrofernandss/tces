import pandas as pd


def ler_dados(url: str) -> pd.DataFrame:
    tces_dataframe = pd.read_parquet(url)

    return tces_dataframe


def contar_convenios_por_ministerio(dataframe: pd.DataFrame) -> pd.DataFrame:
    contagem_convenios_por_ministerio = dataframe.groupby(
        ['ministerio', 'ano_referencia', 'partido_ano_referencia'], observed=True).size().to_frame(name='quantidade_convenios')

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
               dataframe['ano_referencia'],
               dataframe['partido_ano_referencia']],
        columns=dataframe['regiao']
    )

    convenios_pivot_regiao.columns = [
        f'qntd_convenios_{r.lower().replace("-", "_")}' for r in convenios_pivot_regiao.columns]

    return convenios_pivot_regiao


def contar_convenios_por_situacao(dataframe: pd.DataFrame) -> pd.DataFrame:

    convenios_pivot_situacao = pd.crosstab(
        index=[dataframe['ministerio'], dataframe['ano_referencia'],
               dataframe['partido_ano_referencia']],
        columns=dataframe['situacao_convenio']
    )

    convenios_pivot_situacao.columns = [f'qtnd_convenios_{m.lower().replace("Ã", "A").replace("Ç", "C").replace(" ", "_")}'
                                        for m in convenios_pivot_situacao.columns
                                        ]

    return convenios_pivot_situacao


def contar_convenios_por_alinhamento(dataframe: pd.DataFrame, coluna_alinhamento: str) -> pd.DataFrame:

    convenios_pivot_alinhamento = pd.crosstab(
        index=[dataframe['ministerio'], dataframe['ano_referencia'],
               dataframe['partido_ano_referencia']],
        columns=dataframe[coluna_alinhamento]
    )

    mapa_alinhamento = {0: 'nao_alinhado', 1: 'alinhado'}
    convenios_pivot_alinhamento.columns = [
        f'qntd_convenios_{mapa_alinhamento.get(col, col)}' for col in convenios_pivot_alinhamento.columns]

    return convenios_pivot_alinhamento


def somar_valor_convenios(dataframe: pd.DataFrame) -> pd.DataFrame:

    if dataframe['valor_convenio_deflacionado'].dtype == 'object' or dataframe['valor_convenio_deflacionado'].dtype.name == 'string':
        dataframe['valor_convenio_deflacionado'] = dataframe['valor_convenio_deflacionado'].astype(
            str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)

    dataframe['valor_convenio_deflacionado'] = pd.to_numeric(
        dataframe['valor_convenio_deflacionado'], errors='coerce')

    df_valor_convenios_somado = dataframe.groupby(['ministerio', 'ano_referencia', 'partido_ano_referencia'], observed=True)[
        'valor_convenio_deflacionado'].sum().to_frame(name='valor_total_convenios')

    df_valor_convenios_somado['valor_total_convenios'] = df_valor_convenios_somado['valor_total_convenios'].astype(
        float)

    return df_valor_convenios_somado


def calcular_media_distancia_ideologica(dataframe: pd.DataFrame, coluna_distancia: str) -> pd.DataFrame:
        
    media_distancia = dataframe.groupby(['ministerio', 'ano_referencia', 'partido_ano_referencia'], observed=True)[
        coluna_distancia].mean().to_frame(name='media_distancia_ideologica')
    
    return media_distancia


def agregar_base_convenios(url: str, coluna_alinhamento: str, coluna_distancia: str) -> pd.DataFrame:
    df_convenios = ler_dados(url)

    df_conv_valores_somado = somar_valor_convenios(df_convenios)
    df_conv_agg_ministerios = contar_convenios_por_ministerio(df_convenios)
    df_conv_agg_regiao = contar_convenios_por_regiao(df_convenios)
    df_conv_agg_situacao = contar_convenios_por_situacao(df_convenios)
    df_conv_agg_alinhamento = contar_convenios_por_alinhamento(
        df_convenios, coluna_alinhamento)
    df_media_distancia = calcular_media_distancia_ideologica(df_convenios, coluna_distancia)

    tces_final = pd.concat([df_conv_agg_ministerios, df_conv_agg_situacao,
                           df_conv_agg_regiao, df_conv_agg_alinhamento, df_conv_valores_somado, df_media_distancia], axis=1)
    tces_final = tces_final.reset_index()

    return tces_final


if __name__ == "__main__":
    tces_database_url = './database/clean/convenios_clean.parquet'

    local_salvamento_gov = './database/aggregated/convenios_aggregated_gov.parquet'
    tces_agregado_gov = agregar_base_convenios(
        tces_database_url, 'alinhamento_municipio_gov_final_contrato', 'distan_ideologia_municipio_gov_federal')
    tces_agregado_gov.to_parquet(local_salvamento_gov, index=False)
    print(f"Base de dados de alinhamento Gov salva em {local_salvamento_gov}!")

    local_salvamento_minist = './database/aggregated/convenios_aggregated_minist.parquet'
    tces_agregado_minist = agregar_base_convenios(
        tces_database_url, 'alinhamento_municipio_minist_final_contrato', 'distan_ideologia_municipio_minist')
    tces_agregado_minist.to_parquet(local_salvamento_minist, index=False)
    print(
        f"Base de dados de alinhamento Ministério salva em {local_salvamento_minist}!")
