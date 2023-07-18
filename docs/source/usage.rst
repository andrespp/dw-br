Utilização do *Data Warehouse*
==============================

.. _installation:

Instância local
----------------

Para definir uma instância local do ``DW-BR`` deve-se clonar o repositório e
rodar os scripts de ETL da ferramenta:

.. code-block:: console

   $ git clone git@github.com:andrespp/dw-br.git
   $ cd dw-br/
   $ docker compose up -d
   $ conda activate dwbr && ./get_ds.py && ./extract_ds.py && ./update-dw

Instância pública
-----------------

>>> import dask.dataframe as dd
>>> df = dd.read_parquet('s3://bucket/my-parquet-data')

Ferramenta de Visualização Dash DW-BR
-------------------------------------

TBD

