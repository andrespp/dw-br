DW-BRA
======

## Introdução

Um Data Warehouse (DW) de conjuntos de dados abertos Brasileiros.

## Implantação de Uso

### Clone do Repositório

```bash
$ git clone https://github.com/andrespp/dw-bra.git
```

### Configurar os parâmetros de conexões

Para configurar o banco de dados que irá receber o DW, as variáveis do arquivo
`config.ini` devem ser definidas.

### Iniciar o SGBD do DW

```bash
$ docker-compose up -d
```

### Construir/Atualizar o DW

```bash
$ make setup
$ make build
```

### Atualizar o DW

```bash
$ make run
```

## Desenvolvimento

### Virtual environment

Configurar o ambiente virtual para desenvolvimento local:

```bash
conda create --name dwbra python=3.8
conda activate dwbra
conda install -c conda-forge --yes --file requirements.txt
```

## Referências
