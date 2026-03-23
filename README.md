🌾 Agro Data Pipeline — IBGE/SIDRA
Scraper da API pública do IBGE/SIDRA para coleta de dados de produção das
principais commodities agrícolas brasileiras.
Pesquisa de referência: PAM — Produção Agrícola Municipal (Tabela 5457)

📋 Sumário

Visão Geral
Estrutura do Projeto
Pré-requisitos
Instalação
Execução
Configuração
Camada Raw — Formatos de Arquivo
Dados Coletados
Commodities Disponíveis
Estrutura do CSV
Logging
Arquitetura do Código
Desafios Técnicos Tratados
Fonte dos Dados


Visão Geral
Este projeto captura dados anuais de produção agrícola diretamente da
API REST do IBGE/SIDRA, sem autenticação e sem bloqueios, e persiste
os dados brutos em três formatos na camada Raw: CSV, JSON e Parquet.

Por que IBGE/SIDRA e não CEPEA?
O site do CEPEA retorna HTTP 403 e timeout para requisições automatizadas,
inclusive com browser headless (Playwright). A API do IBGE/SIDRA é pública,
documentada e responde normalmente.

O que é coletado por produto e ano:
VariávelDescriçãoUnidadearea_colhida_haÁrea colhidaHectaresqtd_produzidaQuantidade produzidaToneladas (varia por produto)rendimento_medio_kg_haRendimento médioKg/havalor_producao_mil_reaisValor da produçãoMil Reais

Estrutura do Projeto
agro-data-pipeline/
│
├── src/
│   ├── teste.py              # coleta dados e salva CSV + JSON
│   ├── gerar_parquet.py      # coleta dados e salva CSV + JSON + Parquet
│   └── raw_layer.py          # módulo reutilizável da camada Raw
│
├── data/
│   └── raw/
│       ├── csv/
│       │   └── ibge_pam_brasil_2023_2024_YYYYMMDD.csv
│       ├── json/
│       │   └── ibge_pam_brasil_2023_2024_YYYYMMDD.json
│       ├── parquet/
│       │   └── ibge_pam_brasil_2023_2024_YYYYMMDD.parquet
│       └── _manifesto.json
│
├── docs/
│   └── camada_raw.md         # decisões de formato e organização AWS S3
│
├── sidra_scraper.log         # log completo (DEBUG) gerado na execução
├── requirements.txt
├── .env.example              # template de variáveis de ambiente
└── README.md

Os arquivos em data/raw/ são ignorados pelo Git (.gitignore).
O arquivo .env nunca deve ser versionado.


Pré-requisitos

Python 3.11+
Conexão com a internet (acesso à API pública do IBGE)


Instalação
bash# 1. Clone o repositório
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

# 4. Configure o .env
copy .env.example .env      # Windows
cp .env.example .env        # Linux / macOS
requirements.txt:
requests==2.32.4
pandas==2.3.1
python-dotenv==1.1.1
pyarrow==21.0.0

Execução
Coleta completa — CSV, JSON e Parquet (recomendado)
bashpython src/gerar_parquet.py
Este script é autossuficiente: coleta os dados da API e salva os 3 formatos
de uma vez em data/raw/, sem depender de nenhum arquivo anterior.
Coleta apenas CSV e JSON
bashpython src/teste.py
Exemplo de saída no terminal:
2026-03-22 21:44:10 [INFO] =======================================================
2026-03-22 21:44:10 [INFO] IBGE/SIDRA — Camada Raw (CSV + JSON + Parquet)
2026-03-22 21:44:10 [INFO] Anos  : [2023, 2024]
2026-03-22 21:44:10 [INFO] Nível : brasil (n1/all)
2026-03-22 21:44:11 [INFO] Coletando 20 produtos | 3 blocos | anos: [2023, 2024]
2026-03-22 21:44:11 [INFO] Bloco 1/3: soja, milho, algodao, amendoim, arroz, feijao, cafe, trigo
2026-03-22 21:44:13 [INFO] Bloco 1/3: 64 registros recebidos.
2026-03-22 21:44:15 [INFO] CSV     → data\raw\csv\ibge_pam_brasil_2023_2024_20260322.csv  (8.4 KB)
2026-03-22 21:44:15 [INFO] JSON    → data\raw\json\ibge_pam_brasil_2023_2024_20260322.json  (21.6 KB)
2026-03-22 21:44:15 [INFO] Parquet → data\raw\parquet\ibge_pam_brasil_2023_2024_20260322.parquet  (4.6 KB)
2026-03-22 21:44:15 [INFO] Manifesto → data\raw\_manifesto.json
2026-03-22 21:44:15 [INFO] Concluído — 3 formatos salvos em data\raw

Configuração
As opções são lidas do arquivo .env. Copie o .env.example e ajuste:
dotenv# Fonte de dados
API_BASE_URL=https://apisidra.ibge.gov.br/values
TABELA_SIDRA=5457

# Coleta
ANOS=2023,2024
NIVEL=brasil          # brasil | uf | municipio

# Saída
OUTPUT_DIR=data/raw
ParâmetroOpçõesDescriçãoANOSlista separada por vírgulaAnos da série histórica desejadaNIVELbrasil1 linha por produto/ano (nível nacional)uf27 linhas por produto/ano (por estado)municipioaté 5.565 linhas por produto/ano (por município)OUTPUT_DIRqualquer caminhoPasta raiz onde os subdiretórios serão criados

⚠️ O nível municipio gera arquivos grandes. Para 20 produtos × 2 anos,
são aproximadamente 200.000 linhas no Parquet final.


Camada Raw — Formatos de Arquivo
Os dados brutos são salvos em 3 formatos dentro de data/raw/, cada um
em sua própria subpasta. Todos os arquivos compartilham o mesmo nome base,
diferindo apenas na extensão:
ibge_pam_{nivel}_{anos}_{YYYYMMDD}.{ext}
Comparativo de formatos
CritérioCSVJSONParquetLegível por humanos✅✅❌Preserva tipos de dado❌⚠️ parcial✅Compressão nativa❌❌✅ SnappyLeitura colunar❌❌✅Compatível com Athena/Spark⚠️⚠️✅Ideal para auditoria manual✅✅❌
Tamanho real (20 produtos × 2 anos × nível brasil)
FormatoTamanhoRelativo ao CSVCSV8,4 KB100%JSON21,6 KB257%Parquet (Snappy)~4,6 KB~55%

Em datasets com milhões de linhas (nível município), o Parquet chega a ser
10–20× menor que CSV pela combinação de armazenamento colunar e compressão.

Por que salvar os 3 formatos?

CSV — auditoria manual, abertura no Excel, integração com sistemas legados
JSON — envelope com metadados da coleta embutidos (fonte, URL, timestamp)
Parquet — padrão de Data Lakes, compatível com AWS Athena, Spark e DuckDB

Para detalhes sobre organização no AWS S3 e consulta via Athena,
veja docs/camada_raw.md.
Manifesto (_manifesto.json)
Cada execução gera um manifesto na raiz de data/raw/ com metadados
de todos os arquivos salvos:
json{
  "pipeline": "agro-data-pipeline",
  "camada": "raw",
  "fonte": "IBGE/PAM-SIDRA",
  "tabela": "5457",
  "nivel": "brasil",
  "anos": [2023, 2024],
  "gerado_em": "2026-03-22 21:44:15",
  "arquivos": {
    "csv":     { "path": "data/raw/csv/ibge_pam_brasil_2023_2024_20260322.csv",        "tamanho_kb": 8.22  },
    "json":    { "path": "data/raw/json/ibge_pam_brasil_2023_2024_20260322.json",      "tamanho_kb": 21.60 },
    "parquet": { "path": "data/raw/parquet/ibge_pam_brasil_2023_2024_20260322.parquet","tamanho_kb": 4.61  }
  }
}

Dados Coletados
A Tabela 5457 do SIDRA cobre lavouras temporárias e permanentes,
com dados anuais desde 1974 até o ano mais recente publicado.
Exemplo de linha no CSV
produtoanoarea_colhida_haqtd_produzidaunidade_qtdrendimento_medio_kg_havalor_producao_mil_reaisSoja (em grão)202345.056.476162.360.628Toneladas3.604245.876.543Milho (em grão)202322.740.065137.001.311Toneladas6.02677.434.827Café (em grão) Total20232.194.6843.557.390Toneladas1.62149.540.313Cana-de-açúcar20238.614.088672.270.543Toneladas78.04379.131.817

Commodities Disponíveis
Chave internaCódigo SIDRANome oficial IBGEsoja40124Soja (em grão)milho40125Milho (em grão)algodao40126Algodão herbáceo (em caroço)amendoim40127Amendoim (em casca)arroz40128Arroz (em casca)feijao40131Feijão (em grão)cafe40132Café (em grão) Totaltrigo40133Trigo (em grão)cana40135Cana-de-açúcarmandioca40138Mandiocasorgo40141Sorgo (em grão)girassol40147Girassol (em grão)aveia40148Aveia (em grão)cevada40149Cevada (em grão)triticale40150Triticale (em grão)laranja40199Laranjabanana40186Banana (cacho)cacau40190Cacau (em amêndoa)borracha40218Borracha (látex coagulado)sisal40228Sisal ou agave (fibra)

Estrutura do CSV
Nome do arquivo: ibge_pam_{nivel}_{anos}_{YYYYMMDD}.csv
Encoding: UTF-8 com BOM (utf-8-sig) — compatível com Excel sem configuração extra.
ColunaTipoDescriçãoprodutostrNome oficial do produto (ex: "Soja (em grão)")produto_codstrCódigo SIDRA do produto (ex: "40124")nivel_territorialstr"Brasil", "Unidade da Federação" ou "Município"cod_territorialstrCódigo IBGE do territórionome_territorialstrNome do território (ex: "Mato Grosso")anoInt64Ano de referênciaarea_colhida_hafloatÁrea colhida em hectaresqtd_produzidafloatQuantidade produzida (ver unidade_qtd)unidade_qtdstrUnidade de qtd_produzida (ex: "Toneladas", "Mil frutos")rendimento_medio_kg_hafloatRendimento médio em Kg/havalor_producao_mil_reaisfloatValor da produção em Mil ReaisfontestrSempre "IBGE/PAM-SIDRA"tabela_sidrastrSempre "5457"url_origemstrURL exata da requisição feita à APIcoletado_emstrTimestamp da coleta (YYYY-MM-DD HH:MM:SS)

Valores ausentes: células com NaN indicam que o IBGE não publicou
o dado para aquela combinação (produto × ano × território). Na API,
esses casos chegam como "-" (não disponível), "..." (em apuração)
ou "X" (sigiloso — municípios com poucos produtores).


Logging
Todos os scripts utilizam dois handlers simultâneos:
HandlerNívelDestinoConteúdoStreamHandlerINFOTerminalProgresso da coleta e relatório finalFileHandlerDEBUGsidra_scraper.logURLs completas, contagens detalhadas, pausas
Formato:
2026-03-22 21:44:10 [INFO] sidra — Bloco 1/3: soja, milho, algodao ...
2026-03-22 21:44:10 [DEBUG] sidra — GET https://apisidra.ibge.gov.br/values/t/5457/...
Para ver os logs DEBUG no terminal durante o desenvolvimento, altere
o nível do StreamHandler em configurar_logging():
pythonch.setLevel(logging.DEBUG)

Arquitetura do Código
src/teste.py — coleta + CSV e JSON
main()
 ├── coletar()
 │    ├── get_json()       # HTTP GET com retry exponencial
 │    ├── parse_sidra()    # JSON bruto → lista de dicts (formato longo)
 │    │    └── safe_num()  # converte valores especiais da API para float
 │    └── pivotar()        # formato longo → largo (pivot por variável)
 ├── salvar()              # DataFrame → CSV em data/raw/csv/
 └── sumario()             # relatório final via logging
src/gerar_parquet.py — coleta + CSV + JSON + Parquet (recomendado)
main()
 ├── coletar()               # coleta da API IBGE/SIDRA
 ├── salvar_csv()            # → data/raw/csv/
 ├── salvar_json()           # → data/raw/json/  (com envelope de metadados)
 ├── salvar_parquet()        # → data/raw/parquet/  (compressão Snappy)
 └── salvar_manifesto()      # → data/raw/_manifesto.json
src/raw_layer.py — módulo reutilizável da camada Raw
Funções independentes de salvamento que podem ser importadas por outros
scripts do pipeline:
pythonfrom raw_layer import salvar_raw

salvar_raw(df, pasta_base=Path("data/raw"), nivel="brasil", anos=[2023, 2024])
Fluxo de dados
API IBGE/SIDRA
      │
      │  JSON (1 linha por variável × produto × ano × território)
      ▼
parse_sidra()      →   formato longo
      │
      ▼
pivotar()          →   formato largo (1 linha por produto × ano × território)
      │
      ├──► salvar_csv()       →   data/raw/csv/ibge_pam_brasil_2023_2024_YYYYMMDD.csv
      ├──► salvar_json()      →   data/raw/json/ibge_pam_brasil_2023_2024_YYYYMMDD.json
      ├──► salvar_parquet()   →   data/raw/parquet/ibge_pam_brasil_2023_2024_YYYYMMDD.parquet
      └──► salvar_manifesto() →   data/raw/_manifesto.json

Desafios Técnicos Tratados
#DesafioSolução1CEPEA retorna HTTP 403 e timeoutMigração para API pública IBGE/SIDRA2API retorna valores especiais (-, ..., X)safe_num() converte tudo para None / NaN3Formato longo (1 linha por variável)pivotar() transforma em 1 linha por registro4Unidade de qtd_produzida varia por produtoColuna unidade_qtd preservada antes do pivot5URLs longas com muitos produtosColeta em blocos de 8 produtos por requisição6Rate limiting esporádicoRetry com back-off linear (25s × tentativa)7Timeout de redeRetry com back-off (8s × tentativa), até 5 tentativas8Dados sigilosos em municípios pequenos (X)Tratados como None, nunca como zero9Configuração hardcodedVariáveis lidas do .env via python-dotenv10Sem rastreabilidade dos arquivos gerados_manifesto.json criado a cada execução

Fonte dos Dados
ItemDetalhePesquisaProdução Agrícola Municipal (PAM)Tabela SIDRA5457APIhttps://apisidra.ibge.gov.brDocumentação da APIhttps://apisidra.ibge.gov.br/home/ajudaPeriodicidadeAnualSérie histórica1974 – ano mais recente publicadoCoberturaBrasil, 27 UFs, 5.565 municípiosLicença dos dadosCreative Commons BY 4.0