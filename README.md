DW-BRA
======

## Introdução

Um Data Warehouse (DW) de conjuntos de dados abertos Brasileiros.

## Implantação e Utilização

### Provisionamento da Infraestrutura

O DW-BRA poussui scripts para configuração da infra estrutura, utilizando a ferramenta [Ansible](https://www.ansible.com/).

Para confirugação do servidor utilizando estes scritps, inicialmente deve-se definir uma máquina (física, VM, Container) com o sistema operacional [Debian](https://www.debian.org/) e um usuário com poderes de `sudo` definidos.

O arquivo `ansible/inventory` deve ser configurado com o IP do(s) servidor(es), nome do usuário, bem como o caminho para [chave pública ssh](https://www.digitalocean.com/community/tutorials/how-to-configure-ssh-key-based-authentication-on-a-linux-server-pt) autorizada.

Em seguida, basta-se executar o `playbook` Ansible

```bash
$ cd ansible
$ playbook -i inventory main.yml
```

Após a execução, o servidor estará configurado para execução das ferramentas do `DW-BRA`

### Parâmetros de conexões

Para configurar o banco de dados que irá receber o DW, as variáveis do arquivo `config.ini` devem ser definidas.

### Iniciar o SGBD do DW

```bash
$ docker-compose up -d
```

### Construir/Atualizar o DW

```bash
conda activate dwbra && ./get_ds.py && ./extract_ds.py && ./update-dw
```

## Referências
