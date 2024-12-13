Pipeline de Extração Tratamento de Dados - Suprimentos
=============================================================


 O projeto visa a criação e implantação do Data Warehouse do Hospital Prontocardio.
 Este primeiro projeto é um teste de conceita que utiliza um problema real da área de negócio 'suprimentos'
 relacionado a fornecer uma fonte da verdade para que os analistas de dados possam desenvovler as metricas e 
 indicadores relacionados a área.
 

 Configurações:
 =============

<details open>
  <summary>
    <strong>Terminal:</strong>
  </summary>

  - Windows Terminal: É interessante a instalação e configuração do win terminal, pois permite rodar terminais em abas, alterar cores e temas, configurar atalhos e muito mais. O processo é simples e pode ser realizado pela loja de aplicativos da Microsoft. Na sequência é interessante torna-lô terminal padrão.


<details open>
  <summary>
    <strong>WSL:</strong>
  </summary>

  - Windows Subsystem for Linux - WSL 2: O WSL 2 permite execução completa do kernel Linux no windows permitindo melhor desempenho para acessar arquivos e compatibilidade completa de chamada do sistema, além da possibilidade de utilizar Docker nativo (pré-requisito do nosso projeto).

    - O  WSL 2 tem acesso quase que total ao recursos de sua máquina:
        * A 1TB de disco rígido. É criado um disco virtual de 1TB para armazenar os arquivos do Linux (este limite pode ser expandido, ver a área de dicas e truques).
        * A usar completamente os recursos de processamento.
        * A usar 50% da memória RAM disponível.
        * A usar 25% da memória disponível para SWAP (memória virtual).
        
            _- Se você quiser personalizar estes limites, crie um arquivo chamado `.wslconfig` na raiz da sua pasta de usuário `(C:\Users\<seu_usuario>)` e defina estas configurações:_

                ``` bash
                    [wsl2]
                    memory=8GB
                    processors=4
                ```

<details open>
  <summary>
    <strong>Docker Engine:</strong>
  </summary>
    
- Docker Engine: Existem algumas formas para realizar essa instalação, a minha preferida é essa:
- Antes de instalar, verificar instalações de terceiros e desinstalar:



        ``` bash
            for pkg in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc; do sudo apt-get remove $pkg; done
        ```

- Esse comando irá desinstalar os seguintes packages:
    * docker.io
    - docker-compose
    + docker-compose-v2
    * docker-doc
    * podman-docker 


- Instalação com  ['apt repository'](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository) :


        ``` bash
            -- Adicione a chave GPG oficial do Docker
            sudo apt-get update
            sudo apt-get install ca-certificates curl
            sudo install -m 0755 -d /etc/apt/keyrings
            sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
            sudo chmod a+r /etc/apt/keyrings/docker.asc

            -- Adicione o repositório ao Apt sources
            echo \
            "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
            $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
            sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
            sudo apt-get update

        ```


- Verificar se a instalação foi bem sucedida:

        ``` bash
            sudo docker run hello-world
        ```

    > [!TIP]
    > Por padrão o grupo de usuários do Docker é criado, porém sem usuários vinculados.
    > Nesse caso para executar comandos docker será necessário usar a palavra reservada _'sudo'_ ou siga as instruções abaixo:

    - Criar grupo de usuários docker:
        ``` bash
            sudo groupadd docker
        ```
    - Adicionar o usuário ao grupo:
        ``` bash
            sudo usermod -aG docker $USER
        ```
        - _Encerrer a sessão e volte novamente para que a associação seja reavaliada_

    - Ativar as alterações de grupo:
        ``` bash
            newgrp docker
        ```
    
    - Verificar se a configuração foi bem sucedida sem _'sudo'_ :

        ``` bash
            docker run hello-world
        ```
    [Link Configuração Pós Instalação Docker Engine](https://docs.docker.com/engine/install/linux-postinstall/)
    


<details open>
  <summary>
    <strong>Astro:</strong>
  </summary>

- Instalação:

        ``` bash
            curl -sSL install.astronomer.io | sudo bash -s
        ```

- Atualizar:

        ``` bash
            curl -sSL install.astronomer.io | sudo bash -s
        ```

- Desinstalar:

        ``` bash
            sudo rm /usr/local/bin/astro
        ```

- Iniciar um projeto astro:

        ``` bash
            mkdir <nome_do_seu_projeto_astro>
            cd <nome_do_seu_projeto_astro>
            astro dev init
        ```
    - Dentro do diretório criado surgirá uma estrutura parecida com essa:

        ```
        .
        ├── .env # Arquivo contendo suas variáveis de ambiente
        ├── dags # Onde seus arquivos de DAG's devem estar
        │   ├── extracao_suprimentos_MV.py # Exemplos dag's
        │   └── maestro_prontocardio.py # Exemplos dag's
        ├── Dockerfile # Dockerfile para apontar volumes, variáveis de ambiente e outras substituíções.
        ├── include # Para outros scripts
        ├── plugins # Plugins customizados
        │   └── example-plugin.py
        ├── tests # Scripts para testar as dag's
        │   └── test_dag_example.py 
        ├── airflow_settings.yaml # Conexões, variáveis e pools do airflow 
        ├── packages.txt # Packages OS-level
        └── requirements.txt # Packages python
        ```

<details open>
  <summary>
    <strong>dbt-core:</strong>
  </summary>

- Criar ambiente virtual:

        ``` bash
            mkdir <nome_do_seu_projeto_dbt>
            cd <nome_do_seu_projeto_dbt>
            python -m venv dbt-env				# create the environment
        ```

- Ativar Ambiente:

        ``` bash
            source dbt-env/bin/activate			# activate the environment for Mac and Linux OR
            dbt-env\Scripts\activate			# activate the environment for Windows
        ```

- Instalando Adapter Postgres:

        ``` bash
            python -m pip install dbt-core dbt-postgres
        ```
- Verificando as instalações dbt-core & dbt-postgres:

        ``` bash
            $ dbt --version
            installed version: 1.0.0
            latest version: 1.0.0

            Up to date!

            Plugins:
            - postgres: 1.0.0
        ```


- Iniciar um projeto astro:

        ``` bash
            dbt init
        ```

- Testar o projeto:

        ``` bash
            dbt debug
        ```


    - Dentro do diretório criado surgirá uma estrutura parecida com essa:

        ```
        .
        ├── README.md
        ├── analyses
        ├── seeds
        │   └── employees.csv
        ├── dbt_project.yml
        ├── macros
        │   └── customizar_schemas_default.sql
        ├── models
        │   ├── intermediate
        │   │   └── suprimentos
        │   │       ├── _schema.yml
        │   │       └── _int_solicitacao.sql
        │   ├── marts
        │   │   ├── suprimentos
        │   │   │   ├── schema.yml
        │   │   │   ├── pedidos.sql
        │   │   │   └── solicitacao.sql
        │   │   └── marketing
        │   │       ├── _schema.sql
        │   │       └── _marketing.sql
        │   ├── staging
        │   │   ├── suprimentos
        │   │   │   ├── _suprimentos__docs.md
        │   │   │   ├── _stg_pedidos.sql
        │   │   │   ├── _stg_solicitacao.sql
        │   │   │   ├── base
        │   │   │   │   ├── suprimentos__fornecedores.sql
        │   │   │   │   └── suprimentos__deleted_fornecedores.sql
        │   │   │   ├── suprimentos__pedidos.sql
        │   │   │   └── suprimentos__solicitacao.sql
        │   │   └── stripe
        │   │       ├── _stripe__models.yml
        │   │       ├── _stripe__sources.yml
        │   │       └── stg_stripe__payments.sql
        │   └── utilities
        │       └── all_dates.sql
        ├── packages.yml
        ├── snapshots
        └── tests
            └── testes.sql
        ```

</details>















 

