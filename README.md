🌾 Agro Data Pipeline — IBGE/SIDRAPipeline automatizado de extração, tratamento e carga (ETL) para dados de produção das principais commodities agrícolas brasileiras. O projeto utiliza a pesquisa PAM (Produção Agrícola Municipal - Tabela 5457) para consolidar indicadores estratégicos.

📌 Visão GeralEste ecossistema foi desenvolvido para contornar limitações de scrapers convencionais (como bloqueios de 403/Timeout no CEPEA), utilizando a API REST oficial do IBGE. O pipeline opera em arquitetura de medalhão simplificada:Camada Raw: Ingestão de dados brutos em múltiplos formatos (CSV, JSON, Parquet).Camada Processed: Limpeza, tipagem estrita e normalização.Storage/Analytics: Carga final em banco de dados PostgreSQL para consumo via SQL.

🏗️ Arquitetura e Fluxo de DadosO pipeline segue o fluxo: API SIDRA ➔ Raw Layer (Local) ➔ Transformação (Pandas) ➔ PostgreSQL.Snippet de códigograph LR

    A[API IBGE/SIDRA] --> B(Scraper & Raw Layer)
    B --> C{Formatos Raw}
    C -->|CSV| D[ETL Process]
    C -->|JSON| D
    C -->|Parquet| D
    D --> E[(PostgreSQL)]
    D --> F[Processed CSV]

📂 Estrutura de PastasPlaintextagro-data-pipeline/
│
├── src/
│   ├── scraper.py
│   ├── raw_layer.py
│   ├── gerar_parquet.py
│   ├── postgres_load.py
│   └── etl_processed.py
│
├── data/
│   ├── raw/
│   │   ├── csv/
│   │   ├── json/
│   │   ├── parquet/
│   │   └── _manifesto.json
│   └── processed/
│
├── sql/
│   └── verificar_banco.sql
│
├── docs/
│   ├── camada_raw.md
│   └── modelo_relacional.md
│
├── .env
├── .gitignore
├── requirements.txt
├── main.py
└── README.md

🚀 Como Executar
1. Pré-requisitosPython 3.12 ou superior.Instância PostgreSQL (Docker ou Local).
2. Instalação e SetupBash# Clone e entre no diretório
git clone https://github.com/seu-usuario/agro-data-pipeline.git
cd agro-data-pipeline

# Ambiente virtual e dependências
python -m venv venv
source venv/bin/activate  # Linux/macOS ou venv\Scripts\activate no Windows
pip install -r requirements.txt

# Configuração de ambiente
cp .env.example .env
💡 Nota: Edite o arquivo .env com suas credenciais de banco e anos de interesse.3. Execução do PipelineO processo é dividido em dois estágios principais:Ingestão (Raw): Coleta dados da API e gera arquivos locais.Bashpython src/gerar_parquet.py
Processamento (ETL & Load): Limpa os dados e sobe para o Postgres.Bashpython src/etl_processed.py

⚙️ Configurações (.env)VariávelDescriçãoExemploANOSLista de anos para coleta2022,2023,2024NIVELGranularidade geográficabrasil, uf ou municipioDB_NAMENome do banco de dadosagro_db

📊 Estratégia de Dados (Data Dictionary)O pipeline captura as seguintes métricas por produto:MétricaDescriçãoUnidadearea_colhida_haÁrea total colhidaHectaresqtd_produzidaVolume total produzidoToneladas / Mil frutosrendimento_medio_kg_haProdutividade por áreaKg/Havalor_producao_mil_reaisValor nominal da produçãoR$ 1.000,00🛠️ Desafios Técnicos SolucionadosTratamento de Nulos: Conversão automática de caracteres especiais da API (-, ..., X) para NaN/NULL.Resiliência: Implementação de Retry com Back-off (espera progressiva) para evitar quedas por instabilidade na API.Otimização de Memória: Coleta realizada em blocos de 8 produtos para evitar estouro de timeout em URLs longas.Rastreabilidade: Geração automática de um _manifesto.json detalhando data, hora e tamanho dos arquivos gerados.

🔍 Qualidade e LogsO sistema utiliza logs detalhados para auditoria:Terminal (INFO): Progresso visual e relatórios de sucesso.Arquivo sidra_scraper.log (DEBUG): Registro técnico com as URLs de requisição e erros detalhados.Para validar a carga no banco de dados, utilize o script:Bashpsql -U seu_usuario -d seu_banco -f sql/verificar_banco.sql

📄 Licença e FonteFonte: IBGE - Produção Agrícola Municipal (PAM).Licença de Dados: Open Data (CC BY 4.0).Desenvolvido por: [Seu Nome/LinkedIn]

# 📊 Estruturação do Data Lake

## 📌 Visão Geral

O Data Lake deste projeto é organizado em três camadas: **raw**, **processed** e **curated**, com o objetivo de separar os dados conforme o nível de tratamento e uso.

Essa abordagem permite garantir rastreabilidade, qualidade dos dados e suporte à análise.

---

## 🗂️ Camada Raw (dados brutos)

A camada **raw** armazena os dados exatamente como foram coletados da fonte, sem qualquer tipo de transformação.

### Características

- Origem: API pública do IBGE/SIDRA  
- Dados não tratados  
- Preservação completa da estrutura original  
- Possibilidade de reprocessamento  

### Formatos utilizados

- CSV  
- JSON  
- Parquet  

### Localização


data/raw/


### Objetivo

Garantir integridade e rastreabilidade dos dados originais.

---

## 🔄 Camada Processed (dados tratados)

A camada **processed** contém os dados após a execução do processo de ETL.

### Transformações aplicadas

- Correção de tipos de dados  
- Tratamento de valores ausentes  
- Padronização de categorias  
- Validação de regras de negócio  

### Saídas

- Arquivo CSV local:

data/processed/ibge_pam_processed_YYYYMMDD.csv


- Banco de dados PostgreSQL:

schema: processed
tabela: producao_agricola


### Objetivo

Garantir consistência, padronização e qualidade dos dados para uso posterior.

---

## 📊 Camada Curated (dados para análise)

A camada **curated** contém dados preparados para consumo analítico.

### Características

- Dados organizados para facilitar consultas  
- Estrutura otimizada para análise  
- Pode conter agregações ou filtros  

### Localização


data/curated/


### Exemplo


data/curated/producao_agricola_analitico.csv


### Objetivo

Disponibilizar dados prontos para análise e apoio à tomada de decisão.

---

## 📁 Estrutura de Diretórios


agro-data-pipeline/
│
├── data/
│ ├── raw/
│ │ ├── csv/
│ │ ├── json/
│ │ └── parquet/
│ │
│ ├── processed/
│ │ └── ibge_pam_processed_YYYYMMDD.csv
│ │
│ └── curated/
│ └── producao_agricola_analitico.csv


---

## ✅ Resumo

| Camada     | Descrição                    | Objetivo principal              |
|------------|-----------------------------|--------------------------------|
| Raw        | Dados brutos                | Rastreabilidade e auditoria    |
| Processed  | Dados tratados              | Qualidade e consistência       |
| Curated    | Dados para análise          | Suporte à decisão              |

---

## 🎯 Conclusão

A separação em camadas permite organizar o pipeline de dados de forma estruturada, facilitando manutenção, reprocessamento e análise dos dados ao longo do tempo.