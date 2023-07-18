DW-BR
=====

## Introdução

Um Data Warehouse (DW) de conjuntos de dados abertos Brasileiros.

## Implantação e Utilização

### Provisionamento da Infraestrutura

Esta esta é opcional caso você deseje rodar o `DW-BR` localmente em sua máquina.

O DW-BR poussui scripts para configuração da infra estrutura, utilizando a ferramenta [Ansible](https://www.ansible.com/).

Para confirugação do servidor utilizando estes scritps, inicialmente deve-se definir uma máquina (física, VM, Container) com o sistema operacional [Debian](https://www.debian.org/) e um usuário com poderes de `sudo` definidos.

O arquivo `ansible/inventory` deve ser configurado com o IP do(s) servidor(es), nome do usuário, bem como o caminho para [chave pública ssh](https://www.digitalocean.com/community/tutorials/how-to-configure-ssh-key-based-authentication-on-a-linux-server-pt) autorizada.

Em seguida, basta-se executar o `playbook` Ansible

```bash
$ cd ansible
$ playbook -i inventory main.yml
```

Após a execução, o servidor estará configurado para execução das ferramentas do `DW-BR`

### Parâmetros de conexões

Para configurar o banco de dados que irá receber o DW, as variáveis do arquivo `config.ini` devem ser definidas.

As configurações padrão já são funcionais para ambiente local.

### Instalar dependência python

```bash
conda env create -f environment.yml
```

### Construir/Atualizar o DW

```bash
conda activate dwbr && ./get_ds.py && ./extract_ds.py && ./update-dw
```

## Referências

* [PDET - Programa de Disseminação das Estatísticas do Trabalho](http://pdet.mte.gov.br/)
* [PDET - Microdados (FTP)](ftp://ftp.mtps.gov.br/pdet/microdados/)
