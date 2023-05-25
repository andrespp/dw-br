import dask.dataframe as dd
import os.path
import pandas as pd

TABLE_NAME = 'dim_cnae'

###############################################################################
# Extract functions
###############################################################################
def extract(datasrc, verbose=False):
    """Extract data from source

    Parameters
    ----------
        datasrc | parquet path or dw object

        verbose | boolean

    Returns
    -------
        df, df_len
    """

    if(verbose):
        print(f'{TABLE_NAME}: Extract. ', end='', flush=True)

    # Check datasrc
    if os.path.isdir(datasrc): # parquet src
        parquet_table_path = os.path.join(
                datasrc, 'stg_cnaes'
                )
        if os.path.isdir(parquet_table_path):
            df, df_len = extract_parquet(parquet_table_path)
        else:
            raise FileNotFoundError
    else:
        raise NotImplementedError

    if(verbose):
        print('{} registries extracted.'.format(df_len))

    return df, df_len

def extract_parquet(datasrc):

    df = pd.read_parquet(datasrc)
    df_len = len(df)

    return df, df_len

###############################################################################
# Transform functions
###############################################################################
def transform(df, dw=None, dw_sample=None, verbose=False):
    """Transform data

    Parameters
    ----------
        df | Pandas DataFrame

        dw | DataWarehouse object or Path string (parquet target)

        dw_sample | DataWarehouse Object

    Returns
    -------
        data | Pandas or Dask DataFrame
    """
    if(verbose):
        print('{}: Transform. '.format(TABLE_NAME), end='', flush=True)

    ## Enrich data

    sections_data = {
        # Código:Descrição
        'A':'Agricultura, Pecuária, Produção Florestal, Pesca e Aquicultura',
        'B':'Indústrias Extrativas',
        'C':'Indústrias de Transformação',
        'D':'Eletricidade e Gás',
        'E':'Água, Esgoto, Atividades de Gestão de Resíduos e Descontaminação',
        'F':'Construção',
        'G':'Comércio, Reparação de Veículos Automotores e Motocicletas',
        'H':'Transporte, Armazenagem e Correio',
        'I':'Alojamento e Alimentação',
        'J':'Informação e Comunicação',
        'K':'Atividades Financeiras, de Seguros e Serviços Relacionados',
        'L':'Atividades Imobiliárias',
        'M':'Atividades Profissionais, Científicas e Técnicas',
        'N':'Atividades Administrativas e Serviços Complementares',
        'O':'Administração Pública, Defesa e Seguridade Social',
        'P':'Educação',
        'Q':'Saúde Humana e Serviços Sociais',
        'R':'Artes, Cultura, Esporte e Recreação',
        'S':'Outras Atividades de Serviços',
        'T':'Serviços Domésticos',
        'U':'Organismos Internacionais e Outras Instituições Extraterritoriais',
        'Z':'Não identificado',
    }
    sections = pd.DataFrame(
        sections_data.items(), columns=['cod_secao', 'secao']
    )

    # Divisions
    division_data = {
        'A':['01','02', '03'],
        'B':['05','06','07','08','09'],
        'C':['10','11','12','13','14','15','16','17','18','19','20','21','22',
             '23','24','25','26', '27','28','29','30','31','32','33'],
        'D':['35'],
        'E':['36','37','38','39'],
        'F':['41','42','43'],
        'G':['45','46','47'],
        'H':['49','50','51','52','53'],
        'I':['55','56'],
        'J':['58','59','60','61','62','63'],
        'K':['64','65','66'],
        'L':['68'],
        'M':['69','70','71','72','73','74','75'],
        'N':['77','78','79','80','81','82'],
        'O':['84'],
        'P':['85'],
        'Q':['86','87','88'],
        'R':['90','91','92','93'],
        'S':['94','95','96'],
        'T':['97'],
        'U':['99'],
    }
    data = {
        'cod_secao':[],
        'cod_divisao':[],
    }
    for section in division_data:
        for division in division_data[section]:
            data['cod_secao'].append(section)
            data['cod_divisao'].append(division)

    divisions = pd.DataFrame(data)

    ## Transformations

    # cnae_fmt
    # 86.30-5-04 - Atividade odontológica
    df['cnae_fmt'] = df['cnae'].apply(
            lambda x: f'{x[:2]}.{x[2:4]}-{x[4:5]}-{x[5:]}'
    )

    # cod_classe
    # 86.30-5 Atividades de atenção ambulatorial executadas por médicos e odontólogos
    df['cod_classe'] = df['cnae_fmt'].apply(
            lambda x: x[:7]
    )

    # classe
    df['classe'] = 'tbd' # TODO

    # cod_grupo
    # 86.3 Atividades de atenção ambulatorial executadas por médicos e odontólogos
    df['cod_grupo'] = df['cnae_fmt'].apply(
            lambda x: x[:4]
    )

    # grupo
    df['grupo'] = 'tbd' # TODO

    # cod_divisão
    # 86 ATIVIDADES DE ATENÇÃO À SAÚDE HUMANA
    df['cod_divisao'] = df['cnae_fmt'].apply(
            lambda x: x[:2]
    )

    # divisao
    df['divisao'] = 'tbd' # TODO

    # cod_secao
    df = pd.merge(
        df,
        divisions,
        on='cod_divisao',
        how='left',
    )

    # secao
    print(df.shape)
    df = pd.merge(
        df,
        sections,
        on='cod_secao',
        how='left',
    )
    print(df.shape)

    # Set surrogate keys
    df.reset_index(inplace=True, drop=True)
    df['cnae_sk'] = df.index + 1

    # Select and order columns
    df = df[[
        'cnae_sk', 'cnae',
        'cod_secao', 'secao',
        'cod_divisao', 'divisao',
        'cod_grupo', 'grupo',
        'cod_classe', 'classe',
        'cnae_fmt', 'descricao_atividade_economica',
    ]]

    # Dataset len
    df_len = len(df)

    if(verbose):
        print('{} registries transformed.'.format(len(df)))

    return df, df_len

###############################################################################
# Load functions
###############################################################################
def load(df, dw=None, dw_sample=None, verbose=False):
    """Load data into the Data Warehouse

    Parameters
    ----------
        df | Pandas DataFrame
            Data to be loaded

        dw | DataWarehouse object or Path string (parquet target)

        dw_sample | DataWarehouse object

        truncate | boolean
            If true, truncate table before loading data

        verbose | boolean
    """
    if dw_sample: pass

    if(verbose):
        print('{}: Load. '.format(TABLE_NAME), end='', flush=True)

    if isinstance(dw, str): # target=='parquet':

        if(verbose):
            print('(dask) ', end='', flush=True)

        datadir = dw + '/' + TABLE_NAME

        # Write parquet files
        dd.from_pandas(df, npartitions=1).to_parquet(datadir)

    else: # target=='postgres':

        raise NotImplementedError

    # dataset length
    df_len = len(df)

    if(verbose):
        print('{} registries loaded.\n'.format(df_len))

    return df, df_len

