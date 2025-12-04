import pandas as pd

def ler_dados(url: str) -> pd.DataFrame:
    tces_dataframe = pd.read_parquet(url)

    return tces_dataframe

def agrupar_qntd_convenios_ministerio(dataframe: pd.DataFrame) -> pd.DataFrame:
    contagem_convenios_por_ministerio = dataframe.groupby(
    ['ministerio', 'ano_referencia']).size().reset_index(name='quantidade_convenios')

    return contagem_convenios_por_ministerio

def agrupar_qntd_convenios_regiao(dataframe: pd.DataFrame) -> pd.DataFrame:

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