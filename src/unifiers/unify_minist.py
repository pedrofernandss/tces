import pandas as pd

url = './database/aggregated/tces_aggregated_minist.parquet'
tces_dataframe = pd.read_parquet(url)

url = './database/aggregated/convenios_aggregated_minist.parquet'
convenios_dataframe = pd.read_parquet(url)

convenios_dataframe['ano_referencia'] = convenios_dataframe['ano_referencia'].astype('Int64')
tces_dataframe['ano_referencia'] = tces_dataframe['ano_referencia'].astype('Int64')

convenios_dataframe['ministerio'] = convenios_dataframe['ministerio'].astype(str)
tces_dataframe['ministerio'] = tces_dataframe['ministerio'].astype(str)

dataframe_unificado = pd.merge(convenios_dataframe, tces_dataframe, on=[
                               'ministerio', 'ano_referencia', 'partido_ano_referencia', 'media_distancia_ideologica'], how='outer')

dataframe_unificado.to_excel('./bases_unificadas_relacao_minist.xlsx', index=False)