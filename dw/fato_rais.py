"""fato_rais.py
"""
import pandas as pd
import numpy as np

TABLE_NAME = 'fato_rais'
CHUNKSIZE=10**5

def extract(ds_files, verbose=False):
    """Extract data from source

    Parameters
    ----------
        ds_files | cvs list of filenames

    Returns
    -------
        data : Pandas DataFrame
            Extracted Data
    """
    if(verbose):
        print('{}: Extract. '.format(TABLE_NAME), end='', flush=True)

    # dtypes
    dtype={
        'Bairros SP':str,
        'Bairros Fortaleza':str,
        'Bairros RJ':str,
        'Causa Afastamento 1':int,
        'Causa Afastamento 2':int,
        'Causa Afastamento 3':int,
        'Motivo Desligamento':int,
        'CBO Ocupação 2002':str, # CBO 000-1 Impede conversão p/ int
        'CNAE 2.0 Classe':int,
        'CNAE 95 Classe':int,
        'Distritos SP':str,
        'Vínculo Ativo 31/12':int,
        'Faixa Etária':int,
        'Faixa Hora Contrat':int,
        'Faixa Remun Dezem (SM)':int,
        'Faixa Remun Média (SM)':int,
        'Faixa Tempo Emprego':int,
        'Escolaridade após 2005':int,
        'Qtd Hora Contr':int,
        'Idade':int,
        'Ind CEI Vinculado':int,
        'Ind Simples':int,
        'Mês Admissão':int,
        'Mês Desligamento':int,
        'Mun Trab':int,
        'Município':int,
        'Nacionalidade':int,
        'Natureza Jurídica':int,
        'Ind Portador Defic':int,
        'Qtd Dias Afastamento':int,
        'Raça Cor':int,
        'Regiões Adm DF':int,
        'Vl Remun Dezembro Nom':float,
        'Vl Remun Dezembro (SM)':float,
        'Vl Remun Média Nom':float,
        'Vl Remun Média (SM)':float,
        'CNAE 2.0 Subclasse':int,
        'Sexo Trabalhador':int,
        'Tamanho Estabelecimento':int,
        'Tempo Emprego':float,
        'Tipo Admissão':int,
        'Tipo Estab':int,
        'Tipo Estab.1':str,
        'Tipo Defic':int,
        'Tipo Vínculo':int,
        'IBGE Subsetor':int,
        'Vl Rem Janeiro CC':float,
        'Vl Rem Fevereiro CC':float,
        'Vl Rem Março CC':float,
        'Vl Rem Abril CC':float,
        'Vl Rem Maio CC':float,
        'Vl Rem Junho CC':float,
        'Vl Rem Julho CC':float,
        'Vl Rem Agosto CC':float,
        'Vl Rem Setembro CC':float,
        'Vl Rem Outubro CC':float,
        'Vl Rem Novembro CC':float,
        'Ano Chegada Brasil':int,
        'Ind Trab Intermitente':int,
        'Ind Trab Parcial':int,
        'Tipo Salário':int,
        'Vl Salário Contratual':float,
    }


    # Read files
    df_list = []

    for filename in ds_files:
        df = pd.read_csv(filename,
                         encoding='latin1',
                         sep=';',
                         dtype=dtype,
                         thousands='.',
                         decimal=',')
        df_list.append(df)

    # Select common attributes (columns intersection)
    columns = df_list[0].columns
    for i in df_list:
        columns = list(set(columns) & set(i.columns))

    for i in np.arange(len(df_list)):
        df_list[i] = df_list[i][columns]

    # Concatenate datasets
    df = pd.concat(df_list, axis=0, ignore_index=True)

    if(verbose):
        print('{} registries extracted.'.format(len(df)))

    return df


def transform(df, dw, verbose=False):
    """Transform data

    Parameters
    ----------
        df | Pandas DataFrame

        dw | DataWarehouse Object
            Object to be used in data lookups

    Returns
    -------
        data : Pandas DataFrame
            Data to be tranformed
    """
    if(verbose):
        print('{}: Transform. '.format(TABLE_NAME), end='', flush=True)

    # int to boolean
    df['Vínculo Ativo 31/12'] = df['Vínculo Ativo 31/12'].apply(lambda x:
                                                True if x==1 else False)
    df['Ind CEI Vinculado'] = df['Ind CEI Vinculado'].apply(lambda x:
                                                True if x==1 else False)
    df['Ind Simples'] = df['Ind Simples'].apply(lambda x:
                                                True if x==1 else False)
    df['Ind Portador Defic'] = df['Ind Portador Defic'].apply(lambda x:
                                                True if x==1 else False)
    df['Ind Trab Intermitente'] = df['Ind Trab Intermitente'].apply(lambda x:
                                                True if x==1 else False)
    df['Ind Trab Parcial'] = df['Ind Trab Parcial'].apply(lambda x:
                                                True if x==1 else False)

    # Strip white spaces
    df['Tipo Estab.1'] = df['Tipo Estab.1'].apply(lambda x: x.strip())

    # Retrieve Lookup data
    dim_municipio = dw.query("SELECT MUNICIPIO_SK, COD_IBGE FROM DIM_MUNICIPIO")
    dim_municipio['municipio_trab_sk'] = dim_municipio['municipio_sk']
    dim_municipio['Município'] = dim_municipio['cod_ibge'].apply(lambda x:
                                              int(x/10)) # remove last digit
    dim_municipio['Mun Trab'] = dim_municipio['Município']
    dim_municipio['muninipio_trab_sk'] = dim_municipio['municipio_sk']

    # Lookups
    # 'Nacionalidade':int,		# TODO lookup dimension
    # 'Sexo Trabalhador':int,   # TODO dimension lookup
    # 'IBGE Subsetor':int,		# TODO lookup dimension
    # 'Tipo Salário':int,		# TODO lookup dimension

    # municipio_sk
    df = pd.merge(df,
             dim_municipio[['municipio_sk', 'Município']],
             how='left',
             on='Município'
            )

    # municipio_trab_sk
    df = pd.merge(df,
             dim_municipio[['municipio_trab_sk', 'Mun Trab']],
             how='left',
             on='Mun Trab'
            )
    df['municipio_trab_sk'].fillna(value=0, inplace=True) #integer
    df['municipio_trab_sk'] = df['municipio_trab_sk'].apply(int)

    # Rename columns
    df.rename(index=str,
              columns={'Bairros SP':'bairros_sp',
                       'Bairros Fortaleza':'bairros_fortaleza',
                       'Bairros RJ':'bairros_rj',
                       'Causa Afastamento 1':'causa_Afastamento_1',
                       'Causa Afastamento 2':'causa_Afastamento_2',
                       'Causa Afastamento 3':'causa_Afastamento_3',
                       'Motivo Desligamento':'motivo_desligamento',
                       'CBO Ocupação 2002':'cbo_ocupacao_2002',
                       'CNAE 2.0 Classe':'cnae_2_classe',
                       'CNAE 95 Classe':'cnae_95_classe',
                       'Distritos SP':'distritos_sp',
                       'Vínculo Ativo 31/12':'vinculo_ativo_31_12',
                       'Faixa Etária':'faixa_etaria',
                       'Faixa Hora Contrat':'faixa_hora_contrat',
                       'Faixa Remun Dezem (SM)':'faixa_remun_dezem_sm',
                       'Faixa Remun Média (SM)':'faixa_remun_media_sm',
                       'Faixa Tempo Emprego':'faixa_tempo_emprego',
                       'Escolaridade após 2005':'escolaridade_apos_2005',
                       'Qtd Hora Contr':'qtd_hora_contr',
                       'Idade':'idade',
                       'Ind CEI Vinculado':'ind_cei_vinculado',
                       'Ind Simples':'ind_simples',
                       'Mês Admissão':'mes_admissao',
                       'Mês Desligamento':'mes_desligamento',
                       'Mun Trab':'mun_trab',
                       'Município':'municipio',
                       'Nacionalidade':'nacionalidade',
                       'Natureza Jurídica':'natureza_juridica',
                       'Ind Portador Defic':'ind_portador_defic',
                       'Qtd Dias Afastamento':'qtd_dias_afastamento',
                       'Raça Cor':'raca_cor',
                       'Regiões Adm DF':'regioes_adm_df',
                       'Vl Remun Dezembro Nom':'vl_remun_dezembro_nom',
                       'Vl Remun Dezembro (SM)':'vl_remun_dezembro_sm',
                       'Vl Remun Média Nom':'vl_remun_media_nom',
                       'Vl Remun Média (SM)':'vl_remun_media_sm',
                       'CNAE 2.0 Subclasse':'cnae_2_subclasse',
                       'Sexo Trabalhador':'sexo_trabalhador',
                       'Tamanho Estabelecimento':'tamanho_estabelecimento',
                       'Tempo Emprego':'tempo_emprego',
                       'Tipo Admissão':'tipo_admissao',
                       'Tipo Estab':'tipo_estab',
                       'Tipo Estab.1':'tipo_estab_desc',
                       'Tipo Defic':'tipo_defic',
                       'Tipo Vínculo':'tipo_vinculo',
                       'IBGE Subsetor':'ibge_subsetor',
                       'Vl Rem Janeiro CC':'vl_rem_janeiro_cc',
                       'Vl Rem Fevereiro CC':'vl_rem_fevereiro_cc',
                       'Vl Rem Março CC':'vl_rem_marco_cc',
                       'Vl Rem Abril CC':'vl_rem_abril_cc',
                       'Vl Rem Maio CC':'vl_rem_maio_cc',
                       'Vl Rem Junho CC':'vl_rem_junho_cc',
                       'Vl Rem Julho CC':'vl_rem_julho_cc',
                       'Vl Rem Agosto CC':'vl_rem_agosto_cc',
                       'Vl Rem Setembro CC':'vl_rem_setembro_cc',
                       'Vl Rem Outubro CC':'vl_rem_outubro_cc',
                       'Vl Rem Novembro CC':'vl_rem_novembro_cc',
                       'Ano Chegada Brasil':'ano_chegada_brasil',
                       'Ind Trab Intermitente':'ind_trab_intermitente',
                       'Ind Trab Parcial':'ind_trab_parcial',
                       'Tipo Salário':'tipo_salario',
                       'Vl Salário Contratual':'vl_salario_contratual',
                      },
              inplace=True)

    # Select and Reorder columns

    # Lowercase columns names
    df.columns = [x.lower() for x in df.columns]

    # Set surrogate keys
    df.set_index(np.arange(1, len(df)+1), inplace=True)

    if(verbose):
        print('{} registries transformed.'.format(len(df)))

    return df

def load(dw, df, truncate=False, verbose=False):
    """Load data into the Data Warehouse

    Parameters
    ----------
        dw | DataWarehouse object
            DataWarehouse object

        df | Pandas DataFrame
            Data to be loaded

        truncate | boolean
            If true, truncate table before loading data
    """
    if(verbose):
        print('{}: Load. '.format(TABLE_NAME), end='', flush=True)

    # Truncate table
    if truncate:
        dw.truncate(TABLE_NAME)

    dw.write(TABLE_NAME, df, chunksize=CHUNKSIZE)

    if(verbose):
        print('{} registries loaded.\n'.format(len(df)))

    return

