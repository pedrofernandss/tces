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
        'alinhamento.final': 'alinhamento_final',
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


def remover_colunas(database: pd.DataFrame) -> pd.DataFrame:
    colunas_para_remocao = [
        "CÓDIGO SIAFI MUNICÍPIO", "NOME MUNICÍPIO", "NÚMERO ORIGINAL", "NÚMERO PROCESSO DO CONVÊNIO", "OBJETO DO CONVÊNIO",
        "CÓDIGO ÓRGÃO SUPERIOR", "CÓDIGO ÓRGÃO CONCEDENTE", "NOME ÓRGÃO CONCEDENTE", "CÓDIGO UG CONCEDENTE", "NOME UG CONCEDENTE",
        "CÓDIGO CONVENENTE", "TIPO CONVENENTE", "NOME CONVENENTE", "TIPO ENTE CONVENENTE", "VALOR CONVÊNIO",
        "VALOR LIBERADO", "DATA PUBLICAÇÃO", "data_final_vigencia_convenio", "DATA INÍCIO VIGÊNCIA", "DATA ÚLTIMA LIBERAÇÃO",
        "VALOR CONTRAPARTIDA", "VALOR ÚLTIMA LIBERAÇÃO", "T_INICIAL_deflac",
    ]

    database.drop(columns=colunas_para_remocao, inplace=True)

    return database


def padronizar_tipos(database: pd.DataFrame) -> pd.DataFrame:

    database['numero_convenio'] = database['numero_convenio'].astype("Int32")
    database['ministerio'] = database['ministerio'].astype(str).str.title().str.strip()
    database['ministerio'] = database['ministerio'].astype('category')
    database['ano_referencia'] = database['ano_referencia'].astype("Int64")
    database['situacao_convenio'] = database['situacao_convenio'].astype('category')
    database['tipo_instrumento'] = database['tipo_instrumento'].astype('category')
    database['alinhamento_gov'] = database['alinhamento_gov'].astype(pd.Int8Dtype())
    database['alinhamento_final'] = database['alinhamento_final'].astype(pd.Int8Dtype())

    return database

def padronizar_nome_ministerios(database: pd.DataFrame) -> pd.DataFrame:

    relacao_nomes_ministerio = {
        "Ministério Da Agricultura, Pecuária E Abastecimento": "Ministério da Agricultura e Pecuária",
        "Ministério Da Agricultura E Pecuária": "Ministério da Agricultura e Pecuária",
        "Ministério Da Agricultura, Da Pecuária E Do Abastecimento": "Ministério da Agricultura e Pecuária",
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
        "Ministério Dos Direitos Humanos": "Ministério Das Mulheres, Igualdade Racial, Da Juventude E Dos Direitos Humanos",
        "Ministério Da Igualdade Racial": "Ministério Das Mulheres, Igualdade Racial, Da Juventude E Dos Direitos Humanos",
        "Ministério Da Justiça E Segurança Pública": "Ministério Da Justiça",
        "Ministério Da Justiça E Cidadania": "Ministério Da Justiça",
        "Ministério Da Previdência E Assistência Social": "Ministério Da Previdência Social",
        "Ministério De Minas E Energia": "Ministério Das Minas E Energia",
        "Ministério Do Desenvolvimernto Agrário": "Ministério Do Desenvolvimento Social E Agrário",
        "Ministério Do Desenvolvimento Agrário": "Ministério Do Desenvolvimento Social E Agrário",
        "Ministério Do Desenvolvimento Social E Agrário": "Ministério Do Desenvolvimento Social E Agrário",
        "Ministério Do Desenvolvimento Social E Combate À Fome": "Ministério Do Desenvolvimento Social E Agrário",
        "Ministério Do Desenvolvimento Social E Combate A Fome": "Ministério Do Desenvolvimento Social E Agrário",
        "Ministério Do Desenvolvimento Agrário E Agricultura Familiar": "Ministério Do Desenvolvimento Social E Agrário",
        "Ministério Do Desenvolvimento E Assistência Social, Família E Combate À Fome": "Ministério Do Desenvolvimento Social E Agrário",
        "Ministério Do Turismo": "Ministério Do Esporte E Turismo",
        "Ministério Do Esporte": "Ministério Do Esporte E Turismo",
        "Ministério Do Transportes": "Ministério Dos Transportes",
        "Ministério Do Trabalho": "Ministério Do Trabalho E Emprego",
        "Ministério Do Meio Ambiente E Mudança Do Clima": "Ministério Do Meio Ambiente",
        "Ministério Do Planejamento E Orçamento": "Ministério Do Planejamento, Orçamento E Gestão",
        "Ministério Da Gestão E Da Inovação Em Serviços Públicos": "Ministério Do Planejamento, Orçamento E Gestão",
        "Ministério Do Planejamento E Orçamento E Gestão": "Ministério Do Planejamento, Orçamento E Gestão",          
    }

    database['ministerio'] = database['ministerio'].str.replace(r'^.*[-/]\s*', '', regex=True) # Remoção dos acrónimos e o traço/barra   
    database['ministerio'] = database['ministerio'].str.strip() # Remoção de espaços extras no início/fim e normalizar
    database['ministerio'] = database['ministerio'].str.replace(r'Ministérioda', 'Ministério da', regex=True)
    database['ministerio'] = database['ministerio'].str.title() # Converção de tudo para a primeira letra maiúscula e o resto minúsculo
    database['ministerio'] = database['ministerio'].replace(relacao_nomes_ministerio)
    
    database = database[database['ministerio'] != "Sem Informação"]

    return database


def limpar_database_convenios(url) -> pd.DataFrame:

    df_convenios = ler_dados(url)
    df_convenios = formatar_nome_colunas(df_convenios)
    df_convenios = extrair_ano(df_convenios)
    df_convenios = remover_colunas(df_convenios)
    df_convenios = padronizar_nome_ministerios(df_convenios)
    df_convenios = padronizar_tipos(df_convenios)

    return df_convenios


if __name__ == "__main__":
    convenios_database_url = './database/raw/convenios_2025.parquet'
    local_salvamento = './database/clean/convenios_clean.parquet'

    convenios_limpo = limpar_database_convenios(convenios_database_url)
    convenios_limpo.to_parquet(local_salvamento, index=False)

    print(f"Arquivo limpo salvo!")
