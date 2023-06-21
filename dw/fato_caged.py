import dask.dataframe as dd
import os.path
import pandas as pd

TABLE_NAME = 'fato_caged'

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

    src_tables = [
        'dim_municipio',
        'dim_date',
        'dim_cbo2002',
        'dim_sexo',
        'dim_cnae',
        'stg_caged',
    ]

    # Check datasrc
    dfs = {}
    dfs_len = 0
    for table in src_tables:

        if os.path.isdir(datasrc): # parquet src
            parquet_table_path = os.path.join(
                    datasrc, table
                    )
            if os.path.isdir(parquet_table_path):
                df, df_len = extract_parquet(parquet_table_path)
                dfs[table] = df.copy()
                dfs_len += df_len
            else:
                print(f'ERR: "{parquet_table_path}" not found!')
                raise FileNotFoundError
        else:
            raise NotImplementedError

    if(verbose):
        print('{} registries extracted.'.format(dfs_len))

    return dfs, dfs_len

def extract_parquet(datasrc):

    df = dd.read_parquet(datasrc)
    df = df.reset_index(drop=True) # avoid null_dask_index
    df_len = len(df)

    return df, df_len

###############################################################################
# Transform functions
###############################################################################
def transform(dfs, dw=None, dw_sample=None, verbose=False):
    """Transform data

    Parameters
    ----------
        dfs | Dict of Pandas DataFrame

        dw | DataWarehouse object or Path string (parquet target)

        dw_sample | DataWarehouse Object

    Returns
    -------
        data | Pandas or Dask DataFrame
    """
    if(verbose):
        print('{}: Transform. '.format(TABLE_NAME), end='', flush=True)

    # read sources
    dim_municipio = dfs['dim_municipio']
    dim_date = dfs['dim_date']
    dim_cbo2002 = dfs['dim_cbo2002']
    dim_sexo = dfs['dim_sexo']
    dim_cnae = dfs['dim_cnae']
    df = dfs['stg_caged']

    # competenciamov
    df['yearmo_competenciamov_sk'] = df['competenciamov'].apply(
        int, meta=('competenciamov', 'int64')
    )

    #competencia_declaracao
    if 'competenciadec' not in df.columns:
        df['competenciaexc'] = -1
    df['yearmo_competencia_declaracao_sk'] = df['competenciadec'].apply(
        int, meta=('competenciamov', 'int64')
    )

    #competencia_exclusao
    if 'competenciaexc' not in df.columns:
        df['competenciaexc'] = -1
    df['yearmo_competencia_exclusao_sk'] = df['competenciaexc'].apply(
        int, meta=('competenciamov', 'int64')
    )

    # municipio_sk
    df['municipio'] = df['municipio'].apply(int, meta=('municipio','int64'))

    dim_municipio['municipio'] = dim_municipio['cod_ibge'].apply(
        lambda x: int(x/10) # remove 7th digit
        ,meta=('cod_ibge', 'int64')
    )
    dim_municipio = dim_municipio[['municipio_sk', 'municipio']]

    df = df.merge(dim_municipio, on='municipio', how='left')
    df['municipio_sk'] = df['municipio_sk'].fillna(-1)
    df['municipio_sk'] = df['municipio_sk'].apply(
        int, meta=('municipio_sk','int64')
    )

    # cnae_sk
    dim_cnae = dim_cnae[['cnae_sk', 'cnae']]
    df['cnae'] = df['subclasse'].apply(
        lambda x: x.zfill(7)
        ,meta=('subclasse', 'str')
    )
    df = df.merge(dim_cnae, on='cnae', how='left')

    # cbo2002_sk
    dim_cbo2002 = dim_cbo2002[['cbo2002_sk', 'ocupacao']]
    dim_cbo2002['ocupacao'] = dim_cbo2002['ocupacao'].apply(
        int, meta=('ocupacao', 'int32')
    )
    df['ocupacao'] = df['cbo2002ocupacao'].apply(
        int, meta=('cbo2002ocupacao', 'int32')
    )
    df = df.merge(dim_cbo2002, on='ocupacao', how='left')
    df['cbo2002_sk'] = df['cbo2002_sk'].fillna(-1)

    # saldomovimentacao -> only cast

    # idade -> cast int
    df['idade'] = df['idade'].fillna(-1)

    # horascontratuais -> only cast
    df['horascontratuais'] = df['horascontratuais'].fillna(-1)

    # salario -> only cast

    # salario_fixo -> cast  float64
    df['salario_fixo'] = df['valorsalariofixo']

    # categoria
    df = df.merge(get_categoria_df(), on='categoria', how='left')

    # grau_de_instrucao
    df = df.rename(columns={"graudeinstrucao": "grau_de_instrucao"})
    df = df.merge(
        get_grau_de_instrucao_df(), on='grau_de_instrucao', how='left'
    )

    # raçacor
    df = df.rename(columns={'racacor': 'raca_cor'})
    df = df.merge(
        get_raca_cor_df(), on='raca_cor', how='left'
    )

    # sexo
    dim_sexo['sexo'] = dim_sexo['sexo_novo_caged_cod']
    df = df.merge(
        dim_sexo[['sexo_sk', 'sexo']], on='sexo', how='left'
    )

    # tipoempregador
    df = df.rename(columns={'tipoempregador':'tipo_empregador'})
    df = df.merge(
        get_tipo_empregador_df(), on='tipo_empregador', how='left'
    )

    # tipoestabelecimento
    df = df.rename(columns={'tipoestabelecimento':'tipo_estabelecimento'})
    df = df.merge(
        get_tipo_estabelecimento_df(), on='tipo_estabelecimento', how='left'
    )

    # tipomovimentação
    df = df.rename(columns={'tipomovimentacao':'tipo_movimentacao'})
    df = df.merge(
        get_tipo_movimentacao_df(), on='tipo_movimentacao', how='left'
    )

    # tipodedeficiência
    df = df.rename(columns={'tipodedeficiencia':'tipo_deficiencia'})
    df = df.merge(
        get_tipo_deficiencia_df(), on='tipo_deficiencia', how='left'
    )

    # indtrabintermitente
    df = df.rename(columns={'indtrabintermitente':'trabalho_intermitente'})
    df = df.merge(
        get_trabalho_intermitente_df(), on='trabalho_intermitente', how='left'
    )

    # indtrabparcial
    df = df.rename(columns={'indtrabparcial':'trabalho_parcial'})
    df = df.merge(
        get_trabalho_parcial_df(), on='trabalho_parcial', how='left'
    )

    # tamestabjan
    df = df.rename(columns={'tamestabjan':'tamanho_estabelecimento_janeiro'})
    df = df.merge(
        get_tamanho_estabelecimento_df(),
        on='tamanho_estabelecimento_janeiro', how='left'
    )

    # indicadoraprendiz
    df = df.rename(columns={'indicadoraprendiz':'aprendiz'})
    df = df.merge(
        get_aprendiz_df(),
        on='aprendiz', how='left'
    )

    # origemdainformação
    df = df.rename(columns={'origemdainformacao':'origem_informacao'})
    df = df.merge(
        get_origem_informacao_df(),
        on='origem_informacao', how='left'
    )

    # indicadordeexclusão
    if 'indicadordeexclusao' in df.columns:
        df = df.rename(columns={'indicadordeexclusao':'indicador_exclusao'})
        df['indicador_exclusao'] = df['indicador_exclusao'].apply(
            lambda x: bool(x),
            meta=('indicador_exclusao', 'bool')
        )
    else:
        df['indicador_exclusao'] = False

    # indicadordeforadoprazo
    if 'indicadordeforadoprazo' in df.columns:
        df = df.rename(columns={'indicadordeforadoprazo':'fora_de_prazo'})
        df['fora_de_prazo'] = df['fora_de_prazo'].apply(
            lambda x: bool(x),
            meta=('fora_de_prazo', 'bool')
        )
    else:
        df['indicador_exclusao'] = False

    # unidadesaláriocódigo
    df = df.rename(columns={'unidadesalariocodigo':'unidade_salario'})
    df = df.merge(
        get_unidade_salario_df(),
        on='unidade_salario', how='left'
    )

    # Final casts
    df = df[[
        'yearmo_competenciamov_sk', 'yearmo_competencia_declaracao_sk',
        'yearmo_competencia_exclusao_sk',
        'municipio_sk', 'cnae_sk', 'cbo2002_sk', 'sexo_sk',
        'saldomovimentacao', 'idade', 'horascontratuais', 'salario',
        'salario_fixo',
        'categoria', 'categoria_desc',
        'grau_de_instrucao', 'grau_de_instrucao_desc',
        'raca_cor', 'raca_cor_desc',
        'tipo_empregador', 'tipo_empregador_desc',
        'tipo_estabelecimento', 'tipo_estabelecimento_desc',
        'tipo_movimentacao', 'tipo_movimentacao_desc',
        'tipo_deficiencia', 'tipo_deficiencia_desc',
        'trabalho_intermitente', 'trabalho_intermitente_desc',
        'trabalho_parcial', 'trabalho_parcial_desc',
        'tamanho_estabelecimento_janeiro',
        'tamanho_estabelecimento_janeiro_desc',
        'aprendiz', 'aprendiz_desc',
        'origem_informacao', 'origem_informacao_desc',
        'indicador_exclusao', 'fora_de_prazo',
        'unidade_salario', 'unidade_salario_desc',
    ]]
    df = df.astype(
        {
            'yearmo_competenciamov_sk':'int32',
            'yearmo_competencia_declaracao_sk':'int32',
            'yearmo_competencia_exclusao_sk':'int32',
            'municipio_sk':'int32',
            'cnae_sk':'int32',
            'cbo2002_sk':'int32',
            'sexo_sk':'int32',
            'saldomovimentacao':'int32',
            'idade':'int32',
            'horascontratuais':'int32',
            'salario':'float64',
            'salario_fixo':'float64',
            'grau_de_instrucao':'int32',
            'raca_cor':'int32',
            'tipo_empregador':'int32',
            'tipo_estabelecimento':'int32',
            'tipo_movimentacao':'int32',
            'tipo_deficiencia':'int32',
            'trabalho_intermitente':'int32',
            'trabalho_parcial':'int32',
            'tamanho_estabelecimento_janeiro':'int32',
            'aprendiz':'int32',
            'origem_informacao':'int32',
            'indicador_exclusao':bool,
            'fora_de_prazo':bool,
            'unidade_salario':'int32',
        }
    )

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

        # Remove old parquet files
        if verbose:
            print(f'Removing old parquet files. ', end='', flush=True)
        import os
        try:
            for f in os.listdir(datadir):
                if f.endswith(".parquet"):
                    os.remove(os.path.join(datadir, f))
        except FileNotFoundError:
            pass

        # Write parquet files
        df.to_parquet(datadir)

    else: # target=='postgres':

        raise NotImplementedError

    # dataset length
    df_len = len(df)

    if(verbose):
        print('{} registries loaded.\n'.format(df_len))

    return df, df_len

###############################################################################
# Lib Functions / Data Enrich fucntions
###############################################################################
def get_categoria_df():
    sections_data = {
        # Código:Descrição
        '101':'Empregado - Geral, inclusive o empregado público da administração direta ou indireta contratado pela CLT',
        '102':'Empregado - Trabalhador rural por pequeno prazo da Lei 11.718/2008',
        '103':'Empregado - Aprendiz',
        '104':'Empregado - Doméstico',
        '105':'Empregado - Contrato a termo firmado nos termos da Lei 9.601/1998',
        '106':'Trabalhador temporário - Contrato nos termos da Lei 6.019/1974',
        '107':'Empregado - Contrato de trabalho Verde e Amarelo - sem acordo para antecipação mensal da multa rescisória do FGTS',
        '108':'Empregado - Contrato de trabalho Verde e Amarelo - com acordo para antecipação mensal da multa rescisória do FGTS',
        '111':'Empregado - Contrato de trabalho intermitente',
        '999':'Não Identificado',
    }
    return pd.DataFrame(
        sections_data.items(), columns=['categoria', 'categoria_desc']
    ).astype(
        {
            'categoria':'int32',
            'categoria_desc':str,

        }
    )

def get_grau_de_instrucao_df():
    sections_data = {
        # Código:Descrição
        1:'Analfabeto',
        2:'Até 5ª Incompleto',
        3:'5ª Completo Fundamental',
        4:'6ª a 9ª Fundamental',
        5:'Fundamental Completo',
        6:'Médio Incompleto',
        7:'Médio Completo',
        8:'Superior Incompleto',
        9:'Superior Completo',
        10:'Mestrado',
        11:'Doutorado',
        80:'Pós-Graduação completa',
        99:'Não Identificado',

    }
    return pd.DataFrame(
        sections_data.items(), columns=[
            'grau_de_instrucao', 'grau_de_instrucao_desc'
            ]
    ).astype(
        {
            'grau_de_instrucao':'int32',
            'grau_de_instrucao_desc':str,

        }
    )

def get_raca_cor_df():
    sections_data = {
        # Código:Descrição
        1:'Branca',
        2:'Preta',
        3:'Parda',
        4:'Amarela',
        5:'Indígena',
        6:'Não informada',
        9:'Não Identificado',
    }
    return pd.DataFrame(
        sections_data.items(), columns=[
            'raca_cor', 'raca_cor_desc'
            ]
    ).astype(
        {
            'raca_cor':'int32',
            'raca_cor_desc':str,

        }
    )

def get_tipo_empregador_df():
    sections_data = {
        # Código:Descrição
        0:'CNPJ RAIZ',
        2:'CPF',
        9:'Não Identificado',
    }
    return pd.DataFrame(
        sections_data.items(), columns=[
            'tipo_empregador', 'tipo_empregador_desc'
            ]
    ).astype(
        {
            'tipo_empregador':'int32',
            'tipo_empregador_desc':str,

        }
    )

def get_tipo_estabelecimento_df():
    sections_data = {
        # Código:Descrição
        1:'CNPJ',
        3:'CAEPF(Cadastro de Atividade Econômica de Pessoa Física)',
        4:'CNO(Cadastro Nacional de Obra)',
        5:'CEI(CAGED)',
        9:'Não Identificado',
    }
    return pd.DataFrame(
        sections_data.items(), columns=[
            'tipo_estabelecimento', 'tipo_estabelecimento_desc'
            ]
    ).astype(
        {
            'tipo_estabelecimento':'int32',
            'tipo_estabelecimento_desc':str,

        }
    )

def get_tipo_movimentacao_df():
    sections_data = {
        # Código:Descrição
        10:'Admissão por primeiro emprego',
        20:'Admissão por reemprego',
        25:'Admissão por contrato trabalho prazo determinado',
        31:'Desligamento por demissão sem justa causa',
        32:'Desligamento por demissão com justa causa',
        33:'Culpa Recíproca',
        35:'Admissão por reintegração',
        40:'Desligamento a pedido',
        43:'Término contrato trabalho prazo determinado',
        45:'Desligamento por Término de contrato',
        50:'Desligamento por aposentadoria',
        60:'Desligamento por morte',
        70:'Admissão por transferência',
        80:'Desligamento por transferência',
        90:'Desligamento por Acordo entre empregado e empregador',
        97:'Admissão de Tipo Ignorado',
        98:'Desligamento de Tipo Ignorado',
        99:'Não Identificado',
    }
    return pd.DataFrame(
        sections_data.items(), columns=[
            'tipo_movimentacao', 'tipo_movimentacao_desc'
            ]
    ).astype(
        {
            'tipo_movimentacao':'int32',
            'tipo_movimentacao_desc':str,

        }
    )

def get_tipo_deficiencia_df():
    sections_data = {
        # Código:Descrição
        0:'Não Deficiente',
        1:'Física',
        2:'Auditiva',
        3:'Visual',
        4:'Intelectual (Mental)',
        5:'Múltipla',
        6:'Reabilitado',
        9:'Não Identificado',
    }
    return pd.DataFrame(
        sections_data.items(), columns=[
            'tipo_deficiencia', 'tipo_deficiencia_desc'
            ]
    ).astype(
        {
            'tipo_deficiencia':'int32',
            'tipo_deficiencia_desc':str,

        }
    )

def get_trabalho_intermitente_df():
    sections_data = {
        # Código:Descrição
        0:'Não',
        1:'Sim',
        9:'Não Identificado',
    }
    return pd.DataFrame(
        sections_data.items(), columns=[
            'trabalho_intermitente', 'trabalho_intermitente_desc'
            ]
    ).astype(
        {
            'trabalho_intermitente':'int32',
            'trabalho_intermitente_desc':str,

        }
    )

def get_trabalho_parcial_df():
    sections_data = {
        # Código:Descrição
        0:'Não',
        1:'Sim',
        9:'Não Identificado',
    }
    return pd.DataFrame(
        sections_data.items(), columns=[
            'trabalho_parcial', 'trabalho_parcial_desc'
            ]
    ).astype(
        {
            'trabalho_parcial':'int32',
            'trabalho_parcial_desc':str,

        }
    )

def get_tamanho_estabelecimento_df():
    sections_data = {
        # Código:Descrição
        1:'Zero',
        2:'De 1 a 4',
        3:'De 5 a 9',
        4:'De 10 a 19',
        5:'De 20 a 49',
        6:'De 50 a 99',
        7:'De 100 a 249',
        8:'De 250 a 499',
        9:'De 500 a 999',
        10:'1000 ou Mais',
        99:'Ignorado',
        98:'Inválido',
        97:'Não se Aplica',
        90:'Não Informado',
    }
    return pd.DataFrame(
        sections_data.items(), columns=[
            'tamanho_estabelecimento_janeiro',
            'tamanho_estabelecimento_janeiro_desc'
            ]
    ).astype(
        {
            'tamanho_estabelecimento_janeiro':'int32',
            'tamanho_estabelecimento_janeiro_desc':str,

        }
    )

def get_aprendiz_df():
    sections_data = {
        # Código:Descrição
        0:'Não',
        1:'Sim',
        9:'Não Identificado',
    }
    return pd.DataFrame(
        sections_data.items(), columns=[
            'aprendiz', 'aprendiz_desc'
            ]
    ).astype(
        {
            'aprendiz':'int32',
            'aprendiz_desc':str,

        }
    )

def get_origem_informacao_df():
    sections_data = {
        # Código:Descrição
        1:'eSocial',
        2:'CAGED',
        3:'EmpregadoWEB',
    }
    return pd.DataFrame(
        sections_data.items(), columns=[
            'origem_informacao', 'origem_informacao_desc'
            ]
    ).astype(
        {
            'origem_informacao':'int32',
            'origem_informacao_desc':str,

        }
    )

def get_unidade_salario_df():
    sections_data = {
        # Código:Descrição
        1:'Hora',
        2:'Dia',
        3:'Semana',
        4:'Quinzena',
        5:'Mês',
        6:'Tarefa',
        7:'Variavel',
        99:'Não Identificado',

    }
    return pd.DataFrame(
        sections_data.items(), columns=[
            'unidade_salario', 'unidade_salario_desc'
            ]
    ).astype(
        {
            'unidade_salario':'int32',
            'unidade_salario_desc':str,

        }
    )
