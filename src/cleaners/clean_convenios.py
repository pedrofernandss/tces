import pandas as pd


def ler_dados(url: str) -> pd.DataFrame:
    convenios_dataframe = pd.read_parquet(url)

    return convenios_dataframe


def formatar_nome_colunas(database: pd.DataFrame) -> pd.DataFrame:
    nomes_formatados = {
        'NÚMERO CONVÊNIO': 'numero_convenio',
        'UF': 'unidade_federativa',
        "SITUAÇÃO CONVÊNIO": "situacao_convenio",
        'NOME ÓRGÃO SUPERIOR': 'ministerio',
        'valor_convenio_defla': 'valor_convenio_deflacionado',
        'DATA FINAL VIGÊNCIA': 'data_final_vigencia_convenio',
        'alinhamento.gov': 'alinhamento_gov',
        'alinhamento.final': 'alinhamento_final'
    }

    database.rename(columns=nomes_formatados, inplace=True)

    return database


def extrair_ano(database: pd.DataFrame) -> pd.DataFrame:
    database["data_final_vigencia_convenio"] = pd.to_datetime(database["data_final_vigencia_convenio"], dayfirst=True)
    database["ano_final_vigencia_convenio"] = pd.DatetimeIndex(database["data_final_vigencia_convenio"]).year

    return database


def remover_colunas(database: pd.DataFrame) -> pd.DataFrame:
    colunas_para_remocao = [
        "CÓDIGO SIAFI MUNICÍPIO", "NOME MUNICÍPIO", "NÚMERO ORIGINAL", "NÚMERO PROCESSO DO CONVÊNIO", "OBJETO DO CONVÊNIO",
        "CÓDIGO ÓRGÃO SUPERIOR", "CÓDIGO ÓRGÃO CONCEDENTE", "NOME ÓRGÃO CONCEDENTE", "CÓDIGO UG CONCEDENTE", "NOME UG CONCEDENTE",
        "CÓDIGO CONVENENTE", "TIPO CONVENENTE", "NOME CONVENENTE", "TIPO ENTE CONVENENTE", "TIPO INSTRUMENTO", "VALOR CONVÊNIO",
        "VALOR LIBERADO", "DATA PUBLICAÇÃO", "data_final_vigencia_convenio","DATA INÍCIO VIGÊNCIA", "DATA ÚLTIMA LIBERAÇÃO",
        "VALOR CONTRAPARTIDA", "VALOR ÚLTIMA LIBERAÇÃO", "T_INICIAL_deflac",
    ]

    database.drop(columns=colunas_para_remocao, inplace=True)

    return database


def padronizar_tipos(database: pd.DataFrame) -> pd.DataFrame:

    database['ministerio'] = database['ministerio'].astype('category')
    database['numero_convenio'] = database['numero_convenio'].astype("Int64")
    database['situacao_convenio'] = database['situacao_convenio'].astype(
        'category')
    database['alinhamento_gov'] = database['alinhamento_gov'].astype(
        pd.Int8Dtype())
    database['alinhamento_final'] = database['alinhamento_final'].astype(
        pd.Int8Dtype())

    return database


def limpar_database_convenios(url) -> pd.DataFrame:

    df_convenios = ler_dados(url)
    df_convenios = formatar_nome_colunas(df_convenios)
    df_convenios = extrair_ano(df_convenios)
    df_convenios = remover_colunas(df_convenios)
    df_convenios = padronizar_tipos(df_convenios)

    return df_convenios


if __name__ == "__main__":
    convenios_database_url = './database/raw/convenios_2025.parquet'
    local_salvamento = './database/clean/convenios_clean.parquet'

    convenios_limpo = limpar_database_convenios(convenios_database_url)
    convenios_limpo.to_parquet(local_salvamento, index=False)

    print(f"Arquivo limpo salvo!")
