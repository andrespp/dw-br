DW-BRA
======

## Introduction
Build Lototeca's data warehouse from...

## Deploy and Usage

### Clone Repository

```bash
$ git clone
```

### Set Connection Parameters

In order to set up the OLAP database and ETL process, `config.ini` variables must be set.

### Start DW Database

```bash
$ docker-compose up -d
```

### Build/Update the Data Warehouse

**Production Environment**

```bash
$ make setup
$ make build
```

**Development Environment**

```bash
$ make setup
$ make build-dev
```

### Update the Data Warehouse

**Production Environment**

```bash
$ make run
```

**Development Environment**

```bash
$ make run-dev
```

## Development

### Virtual environment

Set Virtual environment for local development:

```bash
conda create --name dwbra python=3.8
conda activate dwbra
conda install -c conda-forge --yes --file requirements.txt
```

## References
