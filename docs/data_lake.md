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