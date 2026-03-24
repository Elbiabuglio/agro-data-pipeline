# 🌾 Agro Data Pipeline — IBGE/SIDRA

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=flat&logo=pandas&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat&logo=Streamlit&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=flat&logo=postgresql&logoColor=white)
![License](https://img.shields.io/badge/License-CC%20BY%204.0-lightgrey.svg)

Pipeline automatizado de extração, transformação e carga (ETL) para dados de produção das principais commodities agrícolas brasileiras com dashboard interativo integrado.

O projeto utiliza a pesquisa **PAM (Produção Agrícola Municipal – Tabela 5457)** como fonte principal para consolidação de indicadores estratégicos e visualização analítica em tempo real.

---

## 📌 Visão Geral
Este pipeline foi desenvolvido para contornar limitações de scrapers convencionais (como bloqueios e timeouts), utilizando a **API REST oficial do IBGE (SIDRA)**. 

A arquitetura segue o modelo de **Medalhão Simplificado**, com separação por camadas:
* **Raw:** Ingestão de dados brutos em múltiplos formatos (CSV, JSON, Parquet).
* **Processed:** Limpeza, tipagem e normalização dos dados.
* **Storage/Analytics:** Carga final em PostgreSQL para consumo analítico via SQL.
* **🆕 Dashboard:** Interface web interativa para visualização e análise dos dados.

---
## 🏗️ Arquitetura do Fluxo

```mermaid
graph TD
    A[API IBGE/SIDRA] --> B[src/gerar_parquet.py]
    B --> C[(Data Lake: Raw)]
    C --> D[src/etl_processed.py]
    D --> E[(Data Lake: Processed)]
    E --> F[(PostgreSQL)]
    F --> G[Dashboard Streamlit]
    E --> H[src/eda.py - Gráficos]

    O Gemini disse
Entendido, Elbia! Peço desculpas, na tentativa de resumir alguns blocos para caber na resposta anterior, acabei removendo as marcações de código (o que faz o GitHub colorir o texto) de algumas seções.

Aqui está o README.md 100% completo, com todos os blocos de código (bash, sql, text) e tabelas formatadas corretamente para que o GitHub renderize tudo "bonito" como você pediu.

Markdown
# 🌾 Agro Data Pipeline — IBGE/SIDRA

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=flat&logo=pandas&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat&logo=Streamlit&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=flat&logo=postgresql&logoColor=white)
![License](https://img.shields.io/badge/License-CC%20BY%204.0-lightgrey.svg)

Pipeline automatizado de extração, transformação e carga (ETL) para dados de produção das principais commodities agrícolas brasileiras com dashboard interativo integrado.

O projeto utiliza a pesquisa **PAM (Produção Agrícola Municipal – Tabela 5457)** como fonte principal para consolidação de indicadores estratégicos e visualização analítica em tempo real.

---

## 📌 Visão Geral
Este pipeline foi desenvolvido para contornar limitações de scrapers convencionais (como bloqueios e timeouts), utilizando a **API REST oficial do IBGE (SIDRA)**. 

A arquitetura segue o modelo de **Medalhão Simplificado**, com separação por camadas:
* **Raw:** Ingestão de dados brutos em múltiplos formatos (CSV, JSON, Parquet).
* **Processed:** Limpeza, tipagem e normalização dos dados.
* **Storage/Analytics:** Carga final em PostgreSQL para consumo analítico via SQL.
* **🆕 Dashboard:** Interface web interativa para visualização e análise dos dados.

---

## 🏗️ Arquitetura do Fluxo

```mermaid
graph TD
    A[API IBGE/SIDRA] --> B[src/gerar_parquet.py]
    B --> C[(Data Lake: Raw)]
    C --> D[src/etl_processed.py]
    D --> E[(Data Lake: Processed)]
    E --> F[(PostgreSQL)]
    F --> G[Dashboard Streamlit]
    E --> H[src/eda.py - Gráficos]

📂 Estrutura de Pastas

agro-data-pipeline/
├── .vscode/                 # Configurações do editor
├── data/                    # Data Lake Local
│   ├── processed/           # Dados tratados e visualizações
│   │   ├── graficos/        # Saídas visuais (PNG) do eda.py
│   │   └── ibge_pam_processed_20260323.csv
│   └── raw/                 # Dados brutos da API
│       ├── csv/ | json/ | parquet/
│       ├── processed/       # Subpasta de controle interno
│       ├── _manifesto.json  # Metadados da última ingestão
│       └── ibge_pam_brasil_2023_2024_20260322.csv
├── docs/                    # Documentação técnica e modelos
├── sql/                     # Scripts DDL e DML (PostgreSQL)
├── src/                     # Core do Pipeline (Scripts Python)
├── streamlit/               # Módulos do Dashboard
│   ├── charts/              # Lógica de geração de gráficos Plotly
│   └── utils/               # Helpers e data loaders
├── venv/                    # Ambiente virtual
├── .env                     # Variáveis de ambiente e credenciais
├── .gitignore               # Arquivos ignorados pelo Git
├── app_dashboard.py         # Main entry do Streamlit
├── main.py                  # Orquestrador principal do Pipeline
├── requirements.txt         # Dependências do projeto
└── run_dashboard.py         # Script de inicialização rápida

## 🚀 Como Executar

### 1. Preparação
Instale as dependências e configure seu arquivo `.env`:
```bash
pip install -r requirements.txt

2. Execução do Pipeline
# Executar fluxo completo (Ingestão + ETL + Carga)
python main.py

# Ou etapas isoladas:
python src/gerar_parquet.py   # Ingestão Raw
python src/etl_processed.py   # Processamento e Carga SQL



3. Inicialização do Dashboard

streamlit run app_dashboard.py

🆕 Dashboard Interativo

📊 Visão Geral: Métricas principais (Área, Produção, Valor) em tempo real.
📈 Análise de Produção: Evolução por commodity e filtros dinâmicos.
🎯 Rendimento: Análise de produtividade e eficiência (kg/ha).
🗺️ Regional: Distribuição geográfica da produção nacional.
📥 Export: Download dos dados filtrados diretamente pela interface.

📊 Análises SQL & Qualidade de Dados
🔎 Exemplos Analíticos
<details>
<summary><b>Clique para expandir as Queries SQL</b></summary>


Variação Percentual (YoY)
SQL
SELECT 
    id_commodity, 
    ano,
    AVG(valor_producao_mil_reais) AS preco_medio,
    LAG(AVG(valor_producao_mil_reais)) OVER (
        PARTITION BY id_commodity 
        ORDER BY ano
    ) AS preco_anterior
FROM fato_producao
GROUP BY id_commodity, ano;
Identificação de AnomaliasSQLSELECT * FROM fato_producao
WHERE area_colhida_ha < 0 
   OR qtd_produzida < 0 
   OR (qtd_produzida * 1000 / NULLIF(area_colhida_ha, 0)) > 100000;
</details>

### ⚡ Performance (Índices)

| Índice | Descrição |
| :--- | :--- |
| `idx_fato_commodity` | Otimiza filtros por produto. |
| `idx_fato_tempo` | Acelera consultas de série histórica (Ano/Mês). |
| `idx_fato_composto` | Melhora performance de funções de janela (Window Functions). |

---

### 🛠️ Desafios Técnicos Solucionados

* **Resiliência de API:** Implementação de **Retry com Back-off** (espera progressiva) para evitar quedas por instabilidade no servidor SIDRA.
* **Tratamento de Dados Sigilosos:** Conversão automática de caracteres especiais do IBGE (`-`, `...`, `X`) para `NULL` para não distorcer médias.
* **Otimização de Armazenamento:** Uso de **Parquet com Snappy compression**, reduzindo o tamanho dos arquivos em até **80%** comparado ao JSON.
* **Rastreabilidade:** Geração de `_manifesto.json` para controle de versões e data de coleta.

---

### 📈 Análise Exploratória (EDA)

O script `src/eda.py` gera insights automáticos salvos em `data/processed/graficos/`:

* **Boxplots:** Distribuição e outliers de produção.
* **Scatter Plots:** Correlação Área vs. Valor de Produção.
* **Estatísticas:** Média, Mediana e Desvio Padrão (Cana e Soja identificadas como principais outliers de volume).

---

### ⚙️ Configurações (.env)

| Variável | Descrição |
| :--- | :--- |
| `ANOS` | Lista de anos (ex: `2023,2024`) |
| `NIVEL` | Granularidade (`brasil`, `uf`, `municipio`) |
| `DB_NAME` | Nome do banco PostgreSQL |
| `STREAMLIT_PORT` | Porta padrão do dashboard (`8501`) |

---

### 📄 Licença e Fonte

* **Fonte:** [IBGE - Produção Agrícola Municipal (PAM)](https://apisidra.ibge.gov.br)
* **Licença:** Open Data (CC BY 4.0)
* **Desenvolvido por:** **Elbia** — *Senior Data Analyst*