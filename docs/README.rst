Para criar uma cópia local da documentação do `DW-BR`, instale os pacotes
indicados em ``requirements-docs.txt`` e execute o comando ``make html``.

Instalação das dependências com ``pip``::

  python -m pip install -r requirements-docs.txt

Depois de rodar o comando ``make html`` o HTML da documentação estará
disponível no diretório ``build/html``. Abra o arquivo
``build/html/index.html`` para visualizá-la.
