Tabelas
=======

.. autosummary::
   :toctree: generated

stg_caged
---------

Origem dos dados
~~~~~~~~~~~~~~~~

* `CAGED <http://pdet.mte.gov.br/microdados-rais-e-caged>`_

Atributos
~~~~~~~~~

Consultas Exemplo
~~~~~~~~~~~~~~~~~

.. code-block:: sql

    SELECT
        COMPETENCIAMOV
        ,SUM(SALDOMOVIMENTACAO) AS SALDOMOV
    FROM STG_CAGED
    GROUP BY COMPETENCIAMOV

dim_municipio
-------------

Origem dos dados - dim_municipio
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* `Municipios Brasileiros <https://www.tesourotransparente.gov.br/ckan/dataset/lista-de-municipios-do-siafi>`_

Atributos - dim_municipio
~~~~~~~~~~~~~~~~~~~~~~~~~

Consultas Exemplo - dim_municipio
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: sql

    SELECT
        UF, COUNT(COD_IBGE) AS QTD_MUNICIPIOS
   FROM DIM_MUNICIPIO
   GROUP BY UF
   ORDER BY COUNT(COD_IBGE)  DESC
