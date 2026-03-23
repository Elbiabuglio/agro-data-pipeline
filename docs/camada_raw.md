# 🗂️ Camada Raw — Organização, Formatos e AWS S3

## 1. Estrutura de Diretórios Local

A camada Raw preserva os dados **exatamente como foram coletados da fonte**,
sem nenhuma transformação. Cada execução gera arquivos nos 3 formatos
em subpastas separadas, com nome que carrega data, nível e anos coletados.

```
agro-data-pipeline/
└── data/
    └── raw/
        ├── csv/
        │   └── ibge_pam_brasil_2023_2024_20260323.csv
        ├── json/
        │   └── ibge_pam_brasil_2023_2024_20260323.json
        ├── parquet/
        │   └── ibge_pam_brasil_2023_2024_20260323.parquet
        └── _manifesto.json
```

### Convenção de nomenclatura

```
ibge_pam_{nivel}_{anos}_{YYYYMMDD}.{ext}
    │       │      │      │
    │       │      │      └── data da coleta (permite múltiplas versões)
    │       │      └───────── anos coletados (ex: 2023_2024)
    │       └──────────────── nível territorial (brasil / uf / municipio)
    └──────────────────────── fonte_pesquisa (ibge_pam = IBGE Prod. Agrícola Municipal)
```

### Manifesto (`_manifesto.json`)

Cada execução gera um manifesto na raiz de `data/raw/` com metadados
de todos os arquivos salvos:

```json
{
  "pipeline": "agro-data-pipeline",
  "camada": "raw",
  "fonte": "IBGE/PAM-SIDRA",
  "tabela": "5457",
  "nivel": "brasil",
  "anos": [2023, 2024],
  "total_linhas": 40,
  "gerado_em": "2026-03-23 21:44:10",
  "arquivos": {
    "csv": {
      "path": "data/raw/csv/ibge_pam_brasil_2023_2024_20260323.csv",
      "formato": "csv",
      "linhas": 40,
      "tamanho_bytes": 8420,
      "tamanho_kb": 8.22,
      "salvo_em": "2026-03-23 21:44:10"
    },
    "json": { "..." },
    "parquet": { "..." }
  }
}
```

---

## 2. Comparação de Formatos

### Resumo executivo

| Critério | CSV | JSON | Parquet |
|----------|:---:|:----:|:-------:|
| Legível por humanos | ✅ | ✅ | ❌ |
| Preserva tipos de dado | ❌ | ⚠️ parcial | ✅ |
| Compressão nativa | ❌ | ❌ | ✅ Snappy/Gzip |
| Leitura colunar | ❌ | ❌ | ✅ |
| Suporte a estruturas aninhadas | ❌ | ✅ | ⚠️ parcial |
| Compatibilidade universal | ✅ | ✅ | ⚠️ requer engine |
| Ideal para Big Data | ❌ | ❌ | ✅ |
| Ideal para auditoria manual | ✅ | ✅ | ❌ |

### Comparação de tamanho (dataset real — 40 linhas × 15 colunas)

| Formato | Tamanho | Relativo ao CSV |
|---------|--------:|:---------------:|
| CSV | 8,4 KB | 100% (baseline) |
| JSON | 21,6 KB | ~257% |
| Parquet (Snappy) | ~4,6 KB | ~55% |

> Em datasets de produção (milhões de linhas), o Parquet chega a ser
> **10–20× menor** que CSV pela combinação de armazenamento colunar e compressão.

---

### 2.1 CSV — *Comma-Separated Values*

```csv
produto,produto_cod,ano,area_colhida_ha,qtd_produzida,valor_producao_mil_reais
Soja (em grão),40124,2023,45056476,162360628,245876543
Milho (em grão),40125,2023,22740065,137001311,77434827
```

**Vantagens:**
- Abre em qualquer ferramenta: Excel, pandas, R, Google Sheets, editor de texto
- Ideal para inspeção e auditoria manual dos dados brutos
- Sem dependência de biblioteca especial para leitura
- Padrão amplamente aceito para troca de dados entre sistemas

**Desvantagens:**
- Sem tipagem: `45056476` pode ser lido como string ou int dependendo da ferramenta
- Sem compressão nativa: maior consumo de disco e banda
- Lento para consultas em colunas específicas em arquivos grandes
- Não suporta estruturas aninhadas (arrays, objetos)

**Quando usar:** auditoria dos dados brutos, compartilhamento com stakeholders
não-técnicos, integrações com sistemas legados.

---

### 2.2 JSON — *JavaScript Object Notation*

```json
{
  "fonte": "IBGE/PAM-SIDRA",
  "tabela": "5457",
  "coletado_em": "2026-03-23 21:44:10",
  "total_registros": 40,
  "registros": [
    {
      "produto": "Soja (em grão)",
      "produto_cod": "40124",
      "ano": 2023,
      "area_colhida_ha": 45056476,
      "qtd_produzida": 162360628,
      "valor_producao_mil_reais": 245876543
    }
  ]
}
```

**Vantagens:**
- Suporta envelope com metadados (fonte, timestamp, versão de API) junto aos dados
- Estruturas aninhadas: um campo pode conter um array ou outro objeto
- Nativo para APIs REST — sem conversão ao consumir ou publicar dados
- Legível por humanos com boas ferramentas (VS Code, jq)

**Desvantagens:**
- Verboso: repete o nome de cada campo em cada registro
- Mais pesado que CSV para dados tabulares planos
- Parsing mais lento em grandes volumes
- Sem compressão nativa

**Quando usar:** APIs, payloads com metadados embutidos, dados com
estrutura hierárquica (ex: múltiplos produtos dentro de uma UF).

---

### 2.3 Parquet — *Apache Parquet*

```
[formato binário colunar — não legível como texto]

Internamente organizado por coluna:
  coluna "produto"    → ["Soja", "Soja", "Milho", "Milho", ...]
  coluna "ano"        → [2023, 2024, 2023, 2024, ...]
  coluna "qtd_produz" → [162360628, null, 137001311, null, ...]

Compressão Snappy aplicada coluna a coluna.
```

**Vantagens:**
- **Armazenamento colunar**: para consultar só `qtd_produzida` de 1M linhas,
  lê apenas essa coluna — sem carregar o restante do arquivo
- **Compressão nativa** (Snappy, Gzip, ZSTD): valores repetidos na mesma
  coluna comprimem muito bem (ex.: "Brasil" repetido 1.000 vezes vira 1)
- **Preserva tipos**: `int64`, `float64`, `datetime`, `bool` são armazenados
  com tipagem precisa — sem risco de inferência errada
- **Padrão de Data Lakes**: compatível com AWS Athena, Spark, DuckDB,
  BigQuery, Redshift Spectrum, Databricks
- **Schema embutido**: o arquivo carrega o schema dos dados dentro de si

**Desvantagens:**
- Requer `pyarrow` ou `fastparquet` para ler/escrever em Python
- Não legível por humanos (binário)
- Overhead de metadados para arquivos muito pequenos (< 1k linhas)

**Quando usar:** camada Raw com grandes volumes (nível município: 5.565
municípios × 20 produtos × N anos), pipelines de processamento com
Spark/Athena, ou quando custo de armazenamento é relevante.

---

### 2.4 Justificativa da escolha para este projeto

| Formato | Uso neste projeto | Motivo |
|---------|-------------------|--------|
| **CSV** | ✅ Formato principal | Auditoria, compartilhamento, compatibilidade universal |
| **JSON** | ✅ Complementar | Preserva metadados da coleta (fonte, URL, timestamp) no mesmo arquivo |
| **Parquet** | ✅ Para escala | Adotado quando o nível for `uf` ou `municipio` (> 100k linhas) |

> Para o nível `brasil` com 2 anos e 20 produtos (40 linhas), CSV é suficiente.
> A camada Raw salva os 3 formatos para garantir flexibilidade downstream.

---

## 3. Organização no AWS S3

### 3.1 Estrutura de buckets recomendada

```
s3://agro-data-pipeline-raw/
└── ibge/
    └── pam/
        └── tabela=5457/
            ├── nivel=brasil/
            │   ├── ano=2023/
            │   │   └── ibge_pam_brasil_2023_20260323.parquet
            │   └── ano=2024/
            │       └── ibge_pam_brasil_2024_20260323.parquet
            ├── nivel=uf/
            │   ├── ano=2023/
            │   │   └── ibge_pam_uf_2023_20260323.parquet
            │   └── ano=2024/
            │       └── ibge_pam_uf_2024_20260323.parquet
            └── nivel=municipio/
                ├── ano=2023/
                │   └── ibge_pam_municipio_2023_20260323.parquet
                └── ano=2024/
                    └── ibge_pam_municipio_2024_20260323.parquet
```

### 3.2 Por que particionar por `nivel=` e `ano=`?

O padrão `chave=valor` nas pastas é reconhecido automaticamente pelo
**AWS Athena** e **AWS Glue** como **partições Hive**.

```sql
-- Sem particionamento: lê TODOS os arquivos do bucket
SELECT * FROM pam WHERE ano = 2023 AND nivel = 'brasil';
-- Lê: 100% dos dados

-- Com particionamento Hive: lê APENAS a pasta relevante
SELECT * FROM pam WHERE ano = 2023 AND nivel = 'brasil';
-- Lê: s3://.../nivel=brasil/ano=2023/ → 1 arquivo
-- Economia: até 99% menos dados lidos → custo Athena proporcional
```

### 3.3 Formato preferido no S3: Parquet

No S3, o formato preferido é **Parquet com compressão Snappy** porque:

1. **Custo de armazenamento**: Parquet é 5–10× menor que CSV
2. **Custo de consulta no Athena**: cobrado por dados varridos — colunar reduz drasticamente
3. **Performance**: leitura paralela por partição com EMR/Glue
4. **Schema Registry**: AWS Glue Catalog detecta schema automaticamente de arquivos Parquet

### 3.4 Configurações de segurança recomendadas

```
Bucket:
  - Bloqueio de acesso público: ATIVADO
  - Versionamento: ATIVADO (permite recuperar versão anterior de uma coleta)
  - Criptografia: SSE-S3 ou SSE-KMS

Ciclo de vida (S3 Lifecycle):
  - Após 30 dias: mover para S3 Standard-IA (menos acessado)
  - Após 90 dias: mover para S3 Glacier Instant Retrieval
  - Raw nunca deletar automaticamente (fonte da verdade)

IAM:
  - Pipeline de coleta: s3:PutObject apenas na pasta /raw/
  - Pipeline de transformação: s3:GetObject em /raw/, s3:PutObject em /trusted/
  - Analistas: s3:GetObject somente leitura
```

### 3.5 Upload com boto3 (Python)

```python
import boto3
from pathlib import Path

s3 = boto3.client("s3")

def upload_raw_s3(arquivo_local: Path, nivel: str, ano: int) -> str:
    """
    Faz upload do arquivo raw para o S3 seguindo a estrutura
    de particionamento Hive.

    Args:
        arquivo_local: Path do arquivo local (.parquet ou .csv)
        nivel:         "brasil" | "uf" | "municipio"
        ano:           Ano de referência dos dados

    Returns:
        s3_uri: URI completa do objeto no S3
    """
    bucket   = "agro-data-pipeline-raw"
    s3_key   = (
        f"ibge/pam/tabela=5457/"
        f"nivel={nivel}/"
        f"ano={ano}/"
        f"{arquivo_local.name}"
    )

    s3.upload_file(
        Filename = str(arquivo_local),
        Bucket   = bucket,
        Key      = s3_key,
        ExtraArgs = {
            "ServerSideEncryption": "AES256",
            "ContentType": "application/octet-stream",
        },
    )

    s3_uri = f"s3://{bucket}/{s3_key}"
    return s3_uri
```

### 3.6 Consulta no AWS Athena após upload

```sql
-- Cria tabela externa apontando para o bucket S3
CREATE EXTERNAL TABLE ibge_pam (
    produto                   STRING,
    produto_cod               STRING,
    cod_territorial           STRING,
    nome_territorial          STRING,
    area_colhida_ha           DOUBLE,
    qtd_produzida             DOUBLE,
    unidade_qtd               STRING,
    rendimento_medio_kg_ha    DOUBLE,
    valor_producao_mil_reais  DOUBLE,
    fonte                     STRING,
    url_origem                STRING,
    coletado_em               STRING
)
PARTITIONED BY (nivel STRING, ano INT)
STORED AS PARQUET
LOCATION 's3://agro-data-pipeline-raw/ibge/pam/tabela=5457/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- Registra as partições automaticamente
MSCK REPAIR TABLE ibge_pam;

-- Consulta eficiente (usa apenas a partição brasil/2023)
SELECT produto, qtd_produzida, valor_producao_mil_reais
FROM ibge_pam
WHERE nivel = 'brasil' AND ano = 2023
ORDER BY valor_producao_mil_reais DESC;
```

---

## 4. Resumo das Decisões

| Decisão | Escolha | Justificativa |
|---------|---------|---------------|
| Formato local principal | CSV | Auditoria e compatibilidade |
| Formato local complementar | JSON | Metadados da coleta embutidos |
| Formato para escala | Parquet + Snappy | Compressão + tipagem + compatibilidade com Athena |
| Particionamento S3 | `nivel=` / `ano=` | Hive-compatible, reduz custo Athena |
| Nomenclatura | `fonte_pesquisa_nivel_anos_data.ext` | Rastreabilidade e idempotência |
| Manifesto | `_manifesto.json` na raiz | Catálogo local de arquivos gerados |
| Retenção Raw | Nunca deletar | Raw é a fonte da verdade — reprocessável |