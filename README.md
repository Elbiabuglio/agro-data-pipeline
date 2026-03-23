# 🌾 Agro Data Pipeline — IBGE/SIDRA

Scraper da **API pública do IBGE/SIDRA** para coleta de dados de produção das
principais commodities agrícolas brasileiras.

Pesquisa de referência: **PAM — Produção Agrícola Municipal (Tabela 5457)**

---

## 📋 Sumário

- [Visão Geral](#visão-geral)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Pré-requisitos](#pré-requisitos)
- [Instalação](#instalação)
- [Execução](#execução)
- [Configuração](#configuração)
- [Dados Coletados](#dados-coletados)
- [Commodities Disponíveis](#commodities-disponíveis)
- [Estrutura do CSV](#estrutura-do-csv)
- [Logging](#logging)
- [Arquitetura do Código](#arquitetura-do-código)
- [Desafios Técnicos Tratados](#desafios-técnicos-tratados)
- [Fonte dos Dados](#fonte-dos-dados)

---

## Visão Geral

Este projeto captura dados anuais de produção agrícola diretamente da
**API REST do IBGE/SIDRA**, sem autenticação e sem bloqueios.

> **Por que IBGE/SIDRA e não CEPEA?**
> O site do CEPEA retorna HTTP 403 e timeout para requisições automatizadas,
> inclusive com browser headless (Playwright). A API do IBGE/SIDRA é pública,
> documentada e responde normalmente.

**O que é coletado por produto e ano:**

| Variável | Descrição | Unidade |
|----------|-----------|---------|
| `area_colhida_ha` | Área colhida | Hectares |
| `qtd_produzida` | Quantidade produzida | Toneladas (varia por produto) |
| `rendimento_medio_kg_ha` | Rendimento médio | Kg/ha |
| `valor_producao_mil_reais` | Valor da produção | Mil Reais |

---

## Estrutura do Projeto

```
agro-data-pipeline/
│
├── src/
│   └── teste.py              # script principal
│
├── data/
│   └── raw/
│       └── ibge_pam_brasil_2023_2024_YYYYMMDD.csv   # saída gerada
│
├── sidra_scraper.log         # log completo (DEBUG) gerado na execução
├── requirements.txt
└── README.md
```

---

## Pré-requisitos

- Python **3.11+**
- Conexão com a internet (acesso à API pública do IBGE)

---

## Instalação

```bash
# 1. Clone o repositório
git clone https://github.com/seu-usuario/agro-data-pipeline.git
cd agro-data-pipeline

# 2. Crie e ative o ambiente virtual
python -m venv venv

# Windows
venv\Scripts\activate

# Linux / macOS
source venv/bin/activate

# 3. Instale as dependências
pip install -r requirements.txt
```

**`requirements.txt`:**

```
requests>=2.31.0
pandas>=2.0.0
```

---

## Execução

```bash
python src/teste.py
```

Só isso. Nenhum argumento necessário.

O script irá:

1. Conectar na API IBGE/SIDRA
2. Coletar todos os produtos para os anos configurados
3. Salvar o CSV em `data/raw/`
4. Exibir o relatório no terminal

**Exemplo de saída no terminal:**

```
2024-03-22 21:44:10 [INFO] sidra — ═══════════════════════════════════════════════════════
2024-03-22 21:44:10 [INFO] sidra — IBGE/SIDRA — PAM Tabela 5457
2024-03-22 21:44:10 [INFO] sidra — Anos    : [2023, 2024]
2024-03-22 21:44:10 [INFO] sidra — Nível   : brasil (n1/all)
2024-03-22 21:44:10 [INFO] sidra — Saída   : C:\...\data\raw
2024-03-22 21:44:10 [INFO] sidra — Iniciando coleta — 20 produtos | 3 blocos | anos: [2023, 2024]
2024-03-22 21:44:11 [INFO] sidra — Bloco 1/3: soja, milho, algodao, amendoim, arroz, feijao, cafe, trigo
2024-03-22 21:44:11 [INFO] sidra — Bloco 1/3: 64 registros recebidos.
...
2024-03-22 21:44:15 [INFO] sidra — CSV salvo em: data\raw\ibge_pam_brasil_2023_2024_20240322.csv
```

---

## Configuração

Todas as opções ficam nas **3 primeiras constantes** do arquivo `src/teste.py`:

```python
ANOS        = [2023, 2024]      # anos a coletar
NIVEL       = "brasil"          # "brasil" | "uf" | "municipio"
PASTA_SAIDA = Path("data/raw")  # destino do CSV
```

| Parâmetro | Opções | Descrição |
|-----------|--------|-----------|
| `ANOS` | qualquer lista de inteiros | Anos da série histórica desejada |
| `NIVEL` | `"brasil"` | 1 linha por produto/ano (nível nacional) |
| | `"uf"` | 27 linhas por produto/ano (por estado) |
| | `"municipio"` | até 5.565 linhas por produto/ano (por município) |
| `PASTA_SAIDA` | qualquer `Path` | Pasta onde o CSV será salvo |

> ⚠️ O nível `"municipio"` gera arquivos grandes. Para 20 produtos × 2 anos,
> são aproximadamente **200.000 linhas** no CSV final.

---

## Dados Coletados

A Tabela 5457 do SIDRA cobre lavouras **temporárias e permanentes**,
com dados anuais desde **1974** até o ano mais recente publicado.

### Exemplo de linha no CSV

| produto | ano | area_colhida_ha | qtd_produzida | unidade_qtd | rendimento_medio_kg_ha | valor_producao_mil_reais |
|---------|-----|----------------:|---------------:|------------|----------------------:|------------------------:|
| Soja (em grão) | 2023 | 45.056.476 | 162.360.628 | Toneladas | 3.604 | 245.876.543 |
| Milho (em grão) | 2023 | 22.740.065 | 137.001.311 | Toneladas | 6.026 | 77.434.827 |
| Café (em grão) Total | 2023 | 2.194.684 | 3.557.390 | Toneladas | 1.621 | 49.540.313 |
| Cana-de-açúcar | 2023 | 8.614.088 | 672.270.543 | Toneladas | 78.043 | 79.131.817 |

---

## Commodities Disponíveis

| Chave interna | Código SIDRA | Nome oficial IBGE |
|---------------|:------------:|-------------------|
| `soja` | 40124 | Soja (em grão) |
| `milho` | 40125 | Milho (em grão) |
| `algodao` | 40126 | Algodão herbáceo (em caroço) |
| `amendoim` | 40127 | Amendoim (em casca) |
| `arroz` | 40128 | Arroz (em casca) |
| `feijao` | 40131 | Feijão (em grão) |
| `cafe` | 40132 | Café (em grão) Total |
| `trigo` | 40133 | Trigo (em grão) |
| `cana` | 40135 | Cana-de-açúcar |
| `mandioca` | 40138 | Mandioca |
| `sorgo` | 40141 | Sorgo (em grão) |
| `girassol` | 40147 | Girassol (em grão) |
| `aveia` | 40148 | Aveia (em grão) |
| `cevada` | 40149 | Cevada (em grão) |
| `triticale` | 40150 | Triticale (em grão) |
| `laranja` | 40199 | Laranja |
| `banana` | 40186 | Banana (cacho) |
| `cacau` | 40190 | Cacau (em amêndoa) |
| `borracha` | 40218 | Borracha (látex coagulado) |
| `sisal` | 40228 | Sisal ou agave (fibra) |

---

## Estrutura do CSV

**Nome do arquivo:** `ibge_pam_{nivel}_{anos}_{YYYYMMDD}.csv`

**Encoding:** UTF-8 com BOM (`utf-8-sig`) — compatível com Excel sem configuração extra.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `produto` | `str` | Nome oficial do produto (ex: `"Soja (em grão)"`) |
| `produto_cod` | `str` | Código SIDRA do produto (ex: `"40124"`) |
| `nivel_territorial` | `str` | `"Brasil"`, `"Unidade da Federação"` ou `"Município"` |
| `cod_territorial` | `str` | Código IBGE do território |
| `nome_territorial` | `str` | Nome do território (ex: `"Mato Grosso"`) |
| `ano` | `Int64` | Ano de referência |
| `area_colhida_ha` | `float` | Área colhida em hectares |
| `qtd_produzida` | `float` | Quantidade produzida (ver `unidade_qtd`) |
| `unidade_qtd` | `str` | Unidade de `qtd_produzida` (ex: `"Toneladas"`, `"Mil frutos"`) |
| `rendimento_medio_kg_ha` | `float` | Rendimento médio em Kg/ha |
| `valor_producao_mil_reais` | `float` | Valor da produção em Mil Reais |
| `fonte` | `str` | Sempre `"IBGE/PAM-SIDRA"` |
| `tabela_sidra` | `str` | Sempre `"5457"` |
| `url_origem` | `str` | URL exata da requisição feita à API |
| `coletado_em` | `str` | Timestamp da coleta (`YYYY-MM-DD HH:MM:SS`) |

> **Valores ausentes:** células com `NaN` indicam que o IBGE não publicou
> o dado para aquela combinação (produto × ano × território). Na API,
> esses casos chegam como `"-"` (não disponível), `"..."` (em apuração)
> ou `"X"` (sigiloso — municípios com poucos produtores).

---

## Logging

O script utiliza dois handlers simultâneos:

| Handler | Nível | Destino | Conteúdo |
|---------|-------|---------|----------|
| `StreamHandler` | `INFO` | Terminal | Progresso da coleta e relatório final |
| `FileHandler` | `DEBUG` | `sidra_scraper.log` | URLs completas, contagens detalhadas, pausas |

**Formato:**
```
2024-03-22 21:44:10 [INFO] sidra — Bloco 1/3: soja, milho, algodao ...
2024-03-22 21:44:10 [DEBUG] sidra — GET https://apisidra.ibge.gov.br/values/t/5457/...
```

Para ver os logs DEBUG no terminal durante o desenvolvimento, altere o nível do `StreamHandler` em `configurar_logging()`:

```python
ch.setLevel(logging.DEBUG)
```

---

## Arquitetura do Código

```
main()
 └── coletar()
      ├── get_json()          # HTTP GET com retry exponencial
      ├── parse_sidra()       # JSON bruto → lista de dicts (formato longo)
      │    └── safe_num()     # converte valores especiais da API para float
      └── pivotar()           # formato longo → largo (pivot por variável)
 └── salvar()                 # DataFrame → CSV em data/raw/
 └── sumario()                # relatório final via logging
```

### Fluxo de dados

```
API IBGE/SIDRA
     │
     │  JSON (1 linha por variável × produto × ano × território)
     ▼
parse_sidra()    →   formato longo
     │
     ▼
pivotar()        →   formato largo (1 linha por produto × ano × território)
     │
     ▼
salvar()         →   data/raw/ibge_pam_brasil_2023_2024_YYYYMMDD.csv
```

---

## Desafios Técnicos Tratados

| # | Desafio | Solução |
|---|---------|---------|
| 1 | **CEPEA retorna HTTP 403 e timeout** | Migração para API pública IBGE/SIDRA |
| 2 | **API retorna valores especiais** (`-`, `...`, `X`) | `safe_num()` converte tudo para `None` / `NaN` |
| 3 | **Formato longo** (1 linha por variável) | `pivotar()` transforma em 1 linha por registro |
| 4 | **Unidade de `qtd_produzida` varia por produto** | Coluna `unidade_qtd` preservada antes do pivot |
| 5 | **URLs longas com muitos produtos** | Coleta em blocos de 8 produtos por requisição |
| 6 | **Rate limiting esporádico** | Retry com back-off linear (25s × tentativa) |
| 7 | **Timeout de rede** | Retry com back-off (8s × tentativa), até 5 tentativas |
| 8 | **Dados sigilosos em municípios pequenos** (`X`) | Tratados como `None`, nunca como zero |

---

## Fonte dos Dados

| Item | Detalhe |
|------|---------|
| **Pesquisa** | Produção Agrícola Municipal (PAM) |
| **Tabela SIDRA** | [5457](https://sidra.ibge.gov.br/tabela/5457) |
| **API** | https://apisidra.ibge.gov.br |
| **Documentação da API** | https://apisidra.ibge.gov.br/home/ajuda |
| **Periodicidade** | Anual |
| **Série histórica** | 1974 – ano mais recente publicado |
| **Cobertura** | Brasil, 27 UFs, 5.565 municípios |
| **Licença dos dados** | [Creative Commons BY 4.0](https://creativecommons.org/licenses/by/4.0/deed.pt_BR) |