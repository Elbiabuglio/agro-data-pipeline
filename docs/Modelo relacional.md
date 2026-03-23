# 🗄️ Modelo Relacional — PostgreSQL

## 1. Diagrama do Modelo

```
┌─────────────────────────┐         ┌──────────────────────────┐
│      dim_commodity      │         │      dim_territorio       │
├─────────────────────────┤         ├──────────────────────────┤
│ PK id_commodity  SERIAL │         │ PK id_territorio  SERIAL │
│    codigo_sidra  VARCHAR│         │    codigo_ibge    VARCHAR │
│    nome_oficial  VARCHAR│         │    nome           VARCHAR │
│    unidade_medida VARCHAR│         │    nivel_territorial      │
└────────────┬────────────┘         └─────────────┬────────────┘
             │                                     │
             │ FK                              FK  │
             └──────────────┬──────────────────────┘
                            │
               ┌────────────▼─────────────────────┐
               │          fato_producao            │
               ├───────────────────────────────────┤
               │ PK id_producao          SERIAL    │
               │ FK id_commodity         INTEGER   │
               │ FK id_territorio        INTEGER   │
               │    ano                  SMALLINT  │
               │    area_colhida_ha      NUMERIC   │
               │    qtd_produzida        NUMERIC   │
               │    rendimento_medio_kg_ha NUMERIC │
               │    valor_producao_mil_reais NUMERIC│
               │    fonte                VARCHAR   │
               │    tabela_sidra         VARCHAR   │
               │    url_origem           TEXT      │
               │    coletado_em          TIMESTAMP │
               └───────────────────────────────────┘
```

---

## 2. Por que 3 tabelas? (Normalização)

O CSV bruto tem esta estrutura **desnormalizada**:

```
produto, produto_cod, nivel_territorial, cod_territorial, nome_territorial,
ano, area_colhida_ha, qtd_produzida, rendimento_medio_kg_ha,
valor_producao_mil_reais, fonte, tabela_sidra, url_origem, coletado_em
```

O problema: `produto`, `produto_cod` e `unidade_medida` se repetem em
**cada linha de produção**. "Soja (em grão)" aparece uma vez por ano.
Isso é redundância — qualquer atualização no nome do produto exigiria
alterar múltiplas linhas.

A normalização resolve isso separando em 3 tabelas com responsabilidades distintas:

| Tabela | Responsabilidade | Granularidade |
|--------|-----------------|---------------|
| `dim_commodity` | O que foi produzido | 1 linha por produto |
| `dim_territorio` | Onde foi produzido | 1 linha por território |
| `fato_producao` | Quanto foi produzido, quando | 1 linha por produto × local × ano |

---

## 3. Justificativa das Chaves

### 3.1 `dim_commodity`

```sql
id_commodity  SERIAL  PRIMARY KEY
codigo_sidra  VARCHAR UNIQUE
```

**`id_commodity` (PK — Surrogate Key):**
Chave artificial gerada pelo banco (`SERIAL`). Não usamos `codigo_sidra`
diretamente como PK porque:
- Códigos externos podem mudar se o IBGE revisar a classificação
- Inteiros são mais eficientes como FK em joins do que strings
- Surrogate keys desacoplam o modelo interno da fonte externa

**`codigo_sidra` (UNIQUE):**
Garante unicidade do código IBGE. Usado como chave de lookup na
carga incremental — `ON CONFLICT (codigo_sidra) DO NOTHING` evita
duplicatas sem precisar checar antes.

---

### 3.2 `dim_territorio`

```sql
id_territorio       SERIAL  PRIMARY KEY
codigo_ibge         VARCHAR NOT NULL
nivel_territorial   VARCHAR NOT NULL
UNIQUE (codigo_ibge, nivel_territorial)
```

**`id_territorio` (PK — Surrogate Key):**
Mesma justificativa que `id_commodity`.

**`UNIQUE (codigo_ibge, nivel_territorial)` (Chave Natural Composta):**
O mesmo código IBGE pode representar entidades diferentes dependendo
do nível territorial. Por exemplo:

```
codigo_ibge = "1"  →  Brasil        (nivel = n1/Brasil)
codigo_ibge = "1"  →  Rondônia      (nivel = n3/UF)
codigo_ibge = "1"  →  Alta Floresta (nivel = n6/Município)
```

A constraint composta garante que a combinação seja única, não o
código isoladamente.

---

### 3.3 `fato_producao`

```sql
id_producao   SERIAL   PRIMARY KEY
id_commodity  INTEGER  REFERENCES dim_commodity(id_commodity)  ON DELETE RESTRICT
id_territorio INTEGER  REFERENCES dim_territorio(id_territorio) ON DELETE RESTRICT
ano           SMALLINT CHECK (ano BETWEEN 1974 AND 2100)
UNIQUE (id_commodity, id_territorio, ano)
```

**`id_producao` (PK — Surrogate Key):**
Identifica unicamente cada medição. Facilita referências futuras
de outras tabelas (ex.: tabela de preços, tabela de exportações).

**`id_commodity` e `id_territorio` (FKs):**
Integridade referencial — o banco impede inserção de produção
para commodity ou território inexistente. Sem FKs, dados órfãos
se acumulariam silenciosamente.

**`ON DELETE RESTRICT`:**
Impede exclusão de uma commodity ou território se houver produção
associada. O histórico de dados é protegido — não é possível
apagar "Soja" e deixar anos de produção sem referência.

**`UNIQUE (id_commodity, id_territorio, ano)` (Chave de Negócio):**
A combinação produto × local × ano é naturalmente única — não existe
duas medições de produção de soja no Brasil em 2023. Esta constraint:
- Impede duplicatas de negócio
- Habilita o `ON CONFLICT DO UPDATE` (UPSERT) na carga incremental
- Garante idempotência: reprocessar o mesmo CSV não duplica dados

**`ano SMALLINT CHECK (1974 AND 2100)`:**
A PAM existe desde 1974. O CHECK previne inserção de anos inválidos
(ex.: 202 por erro de digitação ou parsing incorreto).

---

## 4. Indexes

```sql
idx_fato_ano             ON fato_producao (ano)
idx_fato_commodity       ON fato_producao (id_commodity)
idx_fato_territorio      ON fato_producao (id_territorio)
idx_fato_ano_commodity   ON fato_producao (ano, id_commodity)
```

Os indexes foram criados nas colunas mais usadas em filtros e joins:

- **`ano`** — filtros por período são os mais comuns em análises de série histórica
- **`id_commodity`** — joins com `dim_commodity` em toda query
- **`id_territorio`** — joins com `dim_territorio` em toda query
- **`(ano, id_commodity)`** — index composto para queries do tipo
  "produção de soja em todos os anos" (evita varredura completa)

---

## 5. Configuração do `.env`

Adicione as variáveis de banco no `.env`:

```dotenv
# PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=agro_pipeline
DB_USER=postgres
DB_PASSWORD=sua_senha
```

---

## 6. Execução

```bash
# Instala psycopg2
pip install psycopg2-binary

# Cria banco (se não existir)
psql -U postgres -c "CREATE DATABASE agro_pipeline;"

# Executa carga
python src/postgres_load.py
```

Saída esperada:

```
2026-03-22 22:00:01 [INFO] Conectado ao PostgreSQL: localhost:5432/agro_pipeline
2026-03-22 22:00:01 [INFO] Criando tabelas (IF NOT EXISTS)...
2026-03-22 22:00:01 [INFO]   dim_commodity       ✔
2026-03-22 22:00:01 [INFO]   dim_territorio      ✔
2026-03-22 22:00:01 [INFO]   fato_producao       ✔
2026-03-22 22:00:01 [INFO]   indexes             ✔
2026-03-22 22:00:01 [INFO] dim_commodity  : 20 produtos inseridos/verificados.
2026-03-22 22:00:01 [INFO] dim_territorio :  1 territórios inseridos/verificados.
2026-03-22 22:00:01 [INFO] fato_producao  : 40 linhas inseridas/atualizadas.
2026-03-22 22:00:01 [INFO] Top 5 por valor de produção (2023):
2026-03-22 22:00:01 [INFO]   Soja (em grão)           2023   162.360.628   245.876.543
2026-03-22 22:00:01 [INFO]   Cana-de-açúcar           2023   672.270.543    79.131.817
2026-03-22 22:00:01 [INFO]   Milho (em grão)          2023   137.001.311    77.434.827
2026-03-22 22:00:01 [INFO] FKs órfãs: 0 (OK)
2026-03-22 22:00:01 [INFO] Carga concluída com sucesso.
```

---

## 7. Query de exemplo

```sql
-- Produção de soja e milho em 2023
SELECT
    c.nome_oficial                          AS produto,
    t.nome                                  AS territorio,
    f.ano,
    f.area_colhida_ha,
    f.qtd_produzida,
    f.valor_producao_mil_reais
FROM fato_producao       f
JOIN dim_commodity  c ON c.id_commodity  = f.id_commodity
JOIN dim_territorio t ON t.id_territorio = f.id_territorio
WHERE f.ano = 2023
  AND c.codigo_sidra IN ('40124', '40125')  -- soja, milho
ORDER BY f.valor_producao_mil_reais DESC;
```