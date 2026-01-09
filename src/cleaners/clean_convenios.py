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
        'alinhamento.gov.final': 'alinhamento_municipio_gov_final_contrato',
        'alinhamento.min.pref.1': 'alinhamento_municipio_minist_final_contrato',
        'dista.ideo.min.pref': 'distan_ideologia_municipio_minist',
        'dista.ideo.gov.pref': 'distan_ideologia_municipio_gov_federal',
        "TIPO INSTRUMENTO": "tipo_instrumento"
    }

    database.rename(columns=nomes_formatados, inplace=True)

    return database


def extrair_ano(database: pd.DataFrame) -> pd.DataFrame:
    database["data_final_vigencia_convenio"] = pd.to_datetime(
        database["data_final_vigencia_convenio"], dayfirst=True)
    database["ano_referencia"] = pd.DatetimeIndex(
        database["data_final_vigencia_convenio"]).year

    return database


def filtrar_anos(database: pd.DataFrame) -> pd.DataFrame:
    database = database[(database['ano_referencia'] >= 2002)
                        & (database['ano_referencia'] <= 2016)]
    return database


def remover_colunas(database: pd.DataFrame) -> pd.DataFrame:
    colunas_para_remocao = [
        "CÓDIGO SIAFI MUNICÍPIO", "NOME MUNICÍPIO.x", "NÚMERO ORIGINAL", "NÚMERO PROCESSO DO CONVÊNIO", "OBJETO DO CONVÊNIO",
        "CÓDIGO ÓRGÃO SUPERIOR", "CÓDIGO ÓRGÃO CONCEDENTE", "NOME ÓRGÃO CONCEDENTE", "CÓDIGO UG CONCEDENTE", "NOME UG CONCEDENTE",
        "CÓDIGO CONVENENTE", "TIPO CONVENENTE", "NOME CONVENENTE", "TIPO ENTE CONVENENTE", "VALOR CONVÊNIO",
        "VALOR LIBERADO", "DATA PUBLICAÇÃO", "data_final_vigencia_convenio", "DATA INÍCIO VIGÊNCIA", "DIA_FINAL", "MES_FINAL", "ANO_FINAL", "DATA ÚLTIMA LIBERAÇÃO",
        "VALOR CONTRAPARTIDA", "VALOR ÚLTIMA LIBERAÇÃO", "T_INICIAL_deflac", "ideologia_2000", "ideologia_2004", "ideologia_2008", "ideologia_2012", "alinhamento.final",
        "Partido_2000", "Partido_2004", "Partido_2008", "Partido_2012"
    ]

    database.drop(columns=colunas_para_remocao, inplace=True)

    return database


def padronizar_tipos(database: pd.DataFrame) -> pd.DataFrame:

    cols_to_clean = ['valor_convenio_deflacionado',
                     'distan_ideologia_municipio_minist', 'distan_ideologia_municipio_gov_federal']
    for col in cols_to_clean:
        if col in database.columns:
            if database[col].dtype == 'object' or database[col].dtype.name == 'string':
                database[col] = database[col].astype(str).str.replace(
                    '.', '', regex=False).str.replace(',', '.', regex=False)

    database['numero_convenio'] = database['numero_convenio'].astype("Int32")
    database['ministerio'] = database['ministerio'].astype(
        str).str.title().str.strip()
    database['ministerio'] = database['ministerio'].astype('category')
    database['ano_referencia'] = database['ano_referencia'].astype("Int64")
    database['situacao_convenio'] = database['situacao_convenio'].astype(
        'category')
    database['tipo_instrumento'] = database['tipo_instrumento'].astype(
        'category')
    database['distan_ideologia_municipio_minist'] = pd.to_numeric(
        database['distan_ideologia_municipio_minist'], errors='coerce')
    database['distan_ideologia_municipio_gov_federal'] = pd.to_numeric(
        database['distan_ideologia_municipio_gov_federal'], errors='coerce')
    database['valor_convenio_deflacionado'] = pd.to_numeric(
        database['valor_convenio_deflacionado'], errors='coerce')

    return database


def padronizar_nome_ministerios(database: pd.DataFrame) -> pd.DataFrame:

    relacao_nomes_ministerio = {
        "Ministério Da Agricultura, Pecuária E Abastecimento": "Ministério Da Agricultura E Pecuária",
        "Ministério Da Agricultura E Pecuária": "Ministério Da Agricultura E Pecuária",
        "Ministério Da Agricultura, Da Pecuária E Do Abastecimento": "Ministério Da Agricultura E Pecuária",
        "Ministério Da Ciência E Tecnologia": "Ministério da Ciência e Tecnologia",
        "Ministério Da Ciência, Tecnologia E Inovação": "Ministério da Ciência e Tecnologia",
        "Ministério Da Ciência, Tecnologia, Inovações E Comunicações": "Ministério da Ciência e Tecnologia",
        "Ministério Da Ciência, Tec, Inov. E Com.": "Ministério da Ciência e Tecnologia",
        "Ministério Da Integração E Do Desenvolvimento Regional": "Ministério Da Integração Nacional",
        "Ministério Da Indústria, Comércio Exterior E Serviços": "Ministério Da Indústria, Comércio Exterior E Serviços",
        "Ministério Do Desenvolvimento, Indústria, Comércio E Serviços": "Ministério Da Indústria, Comércio Exterior E Serviços",
        "Ministério Do Desenvolvimento, Indústria E Comércio Exterior": "Ministério Da Indústria, Comércio Exterior E Serviços",
        "Ministério Das Mulheres": "Ministério Das Mulheres, Igualdade Racial, Da Juventude E Dos Direitos Humanos",
        "Ministério Dos Direitos Humanos E Cidadania": "Ministério Das Mulheres, Igualdade Racial, Da Juventude E Dos Direitos Humanos",
        "Ministério Dos Povos Indígenas": "Ministério Das Mulheres, Igualdade Racial, Da Juventude E Dos Direitos Humanos",
        "Ministério Dos Direitos Humanos": "Ministério Das Mulheres, Igualdade Racial, Da Juventude E Dos Direitos Humanos",
        "Ministério Da Igualdade Racial": "Ministério Das Mulheres, Igualdade Racial, Da Juventude E Dos Direitos Humanos",
        "Ministério Da Justiça E Segurança Pública": "Ministério Da Justiça",
        "Ministério Da Justiça E Cidadania": "Ministério Da Justiça",
        "Ministério Da Previdência E Assistência Social": "Ministério Da Previdência Social",
        "Ministério De Minas E Energia": "Ministério Das Minas E Energia",
        "Ministério Do Desenvolvimernto Agrário": "Ministério Do Desenvolvimento Agrário",
        "Ministério Do Desenvolvimento Agrário": "Ministério Do Desenvolvimento Agrário",
        "Ministério Do Desenvolvimento Social E Agrário": "Ministério Do Desenvolvimento Social E Agrário",
        "Ministério Do Desenvolvimento Social E Combate À Fome": "Ministério Do Desenvolvimento Social E Combate A Fome",
        "Ministério Do Desenvolvimento Social E Combate A Fome": "Ministério Do Desenvolvimento Social E Combate A Fome",
        "Ministério Do Desenvolvimento Agrário E Agricultura Familiar": "Ministério Do Desenvolvimento Agrário E Agricultura Familiar",
        "Ministério Do Desenvolvimento Agrário e Agricultura Familiar": "Ministério Do Desenvolvimento Agrário E Agricultura Familiar",
        "Ministério Do Desenvolvimento e Assistência Social, Família e Combate À Fome": "Ministério Do Desenvolvimento E Assistência Social, Família E Combate A Fome",
        "Ministério Do Desenvolvimento E Assistência Social, Família E Combate À Fome": "Ministério Do Desenvolvimento E Assistência Social, Família E Combate A Fome",
        "Ministério Do Transportes": "Ministério Dos Transportes",
        "Ministério Do Trabalho": "Ministério Do Trabalho E Emprego",
        "Ministério Do Meio Ambiente E Mudança Do Clima": "Ministério Do Meio Ambiente",
        "Ministério Do Planejamento E Orçamento": "Ministério Do Planejamento, Orçamento E Gestão",
        "Ministério Da Gestão E Da Inovação Em Serviços Públicos": "Ministério Do Planejamento, Orçamento E Gestão",
        "Ministério Do Planejamento E Orçamento E Gestão": "Ministério Do Planejamento, Orçamento E Gestão",
    }

    database['ministerio'] = database['ministerio'].str.replace(
        r'^.*[-/]\s*', '', regex=True)  # Remoção dos acrónimos e o traço/barra
    # Remoção de espaços extras no início/fim e normalizar
    database['ministerio'] = database['ministerio'].str.strip()
    database['ministerio'] = database['ministerio'].str.replace(
        r'Ministérioda', 'Ministério da', regex=True)
    # Converção de tudo para a primeira letra maiúscula e o resto minúsculo
    database['ministerio'] = database['ministerio'].str.title()
    database['ministerio'] = database['ministerio'].replace(
        relacao_nomes_ministerio)

    database = database[database['ministerio'] != "Sem Informação"]

    return database


def adicionar_partido_ano_referencia(database: pd.DataFrame) -> pd.DataFrame:
    def get_partido(row):
        ano = row['ano_referencia']
        if 2001 <= ano <= 2004:
            return row['Partido_2000']
        elif 2005 <= ano <= 2008:
            return row['Partido_2004']
        elif 2009 <= ano <= 2012:
            return row['Partido_2008']
        elif 2013 <= ano <= 2016:
            return row['Partido_2012']
        return None

    database['partido_ano_referencia'] = database.apply(get_partido, axis=1)
    return database


def limpar_database_convenios(url) -> pd.DataFrame:

    df_convenios = ler_dados(url)
    df_convenios = formatar_nome_colunas(df_convenios)
    df_convenios = extrair_ano(df_convenios)
    df_convenios = filtrar_anos(df_convenios)
    df_convenios = adicionar_partido_ano_referencia(df_convenios)
    df_convenios = remover_colunas(df_convenios)
    df_convenios = padronizar_nome_ministerios(df_convenios)
    df_convenios = padronizar_tipos(df_convenios)

    return df_convenios


if __name__ == "__main__":
    convenios_database_url = './database/raw/convenios.parquet'
    local_salvamento = './database/clean/convenios_clean.parquet'

    convenios_limpo = limpar_database_convenios(convenios_database_url)
    convenios_limpo.to_parquet(local_salvamento, index=False)

    print(f"Arquivo limpo salvo!")
