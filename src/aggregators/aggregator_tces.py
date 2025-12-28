import pandas as pd


def ler_dados(url: str) -> pd.DataFrame:
    tces_dataframe = pd.read_parquet(url)

    return tces_dataframe


def agregar_motivos_tce(motivo: str) -> str:

    motivo_lower = motivo.lower()

    lista_omissao = [
        'omissão no dever de prestar contas',
        'Não encaminhamento da documentação exigida para a prestação de contas',
        'Não encaminhamento de documentação exigida para a prestação de contas'
    ]

    if any(palavra in motivo_lower for palavra in lista_omissao):
        return 'Omissão'

    return 'Outra irregularidade'


def agregar_motivos_instauracao_por_regiao(dataframe: pd.DataFrame):
    tces_motivo_regiao = pd.crosstab(index=[dataframe['ministerio'], dataframe['ano_referencia'], dataframe['partido_ano_referencia']],
                                     columns=[dataframe['motivo_instauracao_tce'], dataframe['regiao']])

    tces_motivo_regiao.columns = [
        f'qntd_motivo_instauracao_{m.lower().replace("Ã", "A").replace("Ç", "C").replace(" ", "_")}_{r.lower()}'
        for m, r in tces_motivo_regiao.columns
    ]

    return tces_motivo_regiao


def agregar_tces_por_alinhamento(dataframe: pd.DataFrame, coluna_alinhamento: str):
    tces_alinhamento = pd.crosstab(index=[dataframe['ministerio'], dataframe['ano_referencia'], dataframe['partido_ano_referencia']],
                                   columns=[dataframe[coluna_alinhamento]])

    alinhamento = {0: 'nao_alinhado', 1: 'alinhado'}
    tces_alinhamento.columns = [
        f'qntd_tces_{alinhamento.get(col, col)}' for col in tces_alinhamento.columns]

    return tces_alinhamento


def calcular_media_distancia_ideologica(dataframe: pd.DataFrame, coluna_distancia: str) -> pd.DataFrame:

    media_distancia = dataframe.groupby(['ministerio', 'ano_referencia', 'partido_ano_referencia'], observed=True)[
        coluna_distancia].mean().to_frame(name='media_distancia_ideologica')

    return media_distancia


def agregar_base_tces(url: str, coluna_alinhamento: str, coluna_distancia: str) -> pd.DataFrame:
    df_tces = ler_dados(url)

    df_tces['motivo_instauracao_tce'] = df_tces['motivo_instauracao_tce'].apply(
        agregar_motivos_tce)
    df_tces_agg_regiao = agregar_motivos_instauracao_por_regiao(df_tces)
    df_tces_agg_alinhamento = agregar_tces_por_alinhamento(
        df_tces, coluna_alinhamento)
    df_media_distancia = calcular_media_distancia_ideologica(
        df_tces, coluna_distancia)

    tces_final = pd.concat(
        [df_tces_agg_regiao, df_tces_agg_alinhamento, df_media_distancia], axis=1)
    tces_final = tces_final.reset_index()

    return tces_final


if __name__ == "__main__":
    tces_database_url = './database/clean/tces_clean.parquet'

    local_salvamento_gov = './database/aggregated/tces_aggregated_gov.parquet'
    tces_agregado_gov = agregar_base_tces(
        tces_database_url, 'alinhamento_municipio_gov_tce', 'distan_ideologia_municipio_gov_federal')
    tces_agregado_gov.to_parquet(local_salvamento_gov, index=False)
    print(f"Base de dados agregada (Gov) salva em {local_salvamento_gov}!")

    local_salvamento_minist = './database/aggregated/tces_aggregated_minist.parquet'
    tces_agregado_minist = agregar_base_tces(
        tces_database_url, 'alinhamento_municipio_minist_tce', 'distan_ideologia_municipio_minist')
    tces_agregado_minist.to_parquet(local_salvamento_minist, index=False)
    print(
        f"Base de dados agregada (Minist) salva em {local_salvamento_minist}!")
