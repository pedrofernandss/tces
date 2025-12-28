import pandas as pd

def ler_dados(url: str) -> pd.DataFrame:
    convenios_dataframe = pd.read_parquet(url)

    return convenios_dataframe


def remover_colunas(database: pd.DataFrame) -> pd.DataFrame:
    colunas_para_remocao = [
        "Tema provável", "Nº do Processo da Unidade Responsável pelo Recurso", "Nº Original do Instrumento de Repasse",
        "Nº SIAFI do Instrumento de Repasse", "Unidade Gestora", "Nº Siconvi", "Unidade", "Valor Original",
        "(c) Valor Corrigido a Ser Restituído aos Cofres Públicos (R$)",
        "code_muni", "name_muni", "code_state", "code_region", "name_state",
        "ideologia_2000", "ideologia_2004", "ideologia_2008", "ideologia_2012",
        "Partido_2000", "Partido_2004", "Partido_2008", "Partido_2012"
    ]

    database.drop(columns=colunas_para_remocao, inplace=True, errors='ignore')

    return database

def formatar_nome_colunas(database: pd.DataFrame) -> pd.DataFrame:
    nomes_formatados = {
        'Ano da instauração do processo pela CGU': 'ano_referencia',
        'UF.x': 'unidade_federativa',
        'name_region': 'regiao',
        'Ministério': 'ministerio',
        'Motivo da Instauração da TCE': 'motivo_instauracao_tce',
        'alinhamento_tce1': 'alinhamento_municipio_gov_tce',
        'alinhamento.min.pref.1': 'alinhamento_municipio_minist_tce',
        'dista.ideo.min.pref': 'distan_ideologia_municipio_minist',
        'dista.ideo.gov.pref': 'distan_ideologia_municipio_gov_federal',
        'governo_tce': 'governo_instaurador_tce'
    }

    database.rename(columns=nomes_formatados, inplace=True)

    return database


def padronizar_tipos(database: pd.DataFrame) -> pd.DataFrame:

    database['ministerio'] = database['ministerio'].astype(
        str).str.title().str.strip()
    database['ministerio'] = database['ministerio'].astype('category')
    database['ano_referencia'] = database['ano_referencia'].astype('Int32')
    database['regiao'] = database['regiao'].astype('category')
    database['unidade_federativa'] = database['unidade_federativa'].astype(
        'category')
    database['motivo_instauracao_tce'] = database['motivo_instauracao_tce'].astype(
        'category')

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

    return database

def adicionar_partido_ano_referencia(database: pd.DataFrame) -> pd.DataFrame:
    def get_partido(row):
        ano = row['ano_referencia']
        if 2001 <= ano <= 2004:
            return row['Partido_2000']
        elif 2005 <= ano <= 2009:
            return row['Partido_2004']
        elif 20010 <= ano <= 2012:
            return row['Partido_2008']
        elif 2013 <= ano <= 2016:
            return row['Partido_2012']
        return None

    database['partido_ano_referencia'] = database.apply(get_partido, axis=1)
    return database

def limpar_database_convenios(url) -> pd.DataFrame:

    df_tces = ler_dados(url)
    df_tces = formatar_nome_colunas(df_tces)
    df_tces = padronizar_tipos(df_tces)
    df_tces = adicionar_partido_ano_referencia(df_tces)
    df_tces = remover_colunas(df_tces)
    df_tces = padronizar_nome_ministerios(df_tces)

    return df_tces


if __name__ == "__main__":
    convenios_database_url = './database/raw/tces.parquet'
    local_salvamento = './database/clean/tces_clean.parquet'

    convenios_limpo = limpar_database_convenios(convenios_database_url)
    convenios_limpo.to_parquet(local_salvamento, index=False)

    print(f"Arquivo limpo salvo!")
