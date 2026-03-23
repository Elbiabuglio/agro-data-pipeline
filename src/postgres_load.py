"""
postgres_load.py
================
Cria o schema normalizado no PostgreSQL e insere os dados
coletados da camada Raw (CSV em data/raw/csv/).

Modelo relacional (3 tabelas normalizadas):

    dim_commodity          dim_territorio
    ─────────────          ──────────────
    PK id_commodity        PK id_territorio
       codigo_sidra           codigo_ibge
       nome_oficial           nome
       unidade_medida         nivel_territorial

           └──────────────┬──────────────┘
                          │
                    fato_producao
                    ─────────────
                    PK id_producao
                    FK id_commodity     → dim_commodity
                    FK id_territorio    → dim_territorio
                       ano
                       area_colhida_ha
                       qtd_produzida
                       rendimento_medio_kg_ha
                       valor_producao_mil_reais
                       fonte
                       tabela_sidra
                       url_origem
                       coletado_em

Execução:
    python src/postgres_load.py
"""

import logging
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()


# ──────────────────────────────────────────────────────────────
#  CONFIGURAÇÃO
# ──────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host"    : os.getenv("DB_HOST",     "localhost"),
    "port"    : int(os.getenv("DB_PORT", "5432")),
    "dbname"  : os.getenv("DB_NAME",     "agro_pipeline"),
    "user"    : os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

PASTA_CSV   = Path(os.getenv("OUTPUT_DIR", "data/raw")) / "csv"
SCHEMA      = "public"


# ──────────────────────────────────────────────────────────────
#  LOGGING
# ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("postgres_load.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("postgres")


# ──────────────────────────────────────────────────────────────
#  DDL — CRIAÇÃO DAS TABELAS
# ──────────────────────────────────────────────────────────────

DDL_DIM_COMMODITY = """
CREATE TABLE IF NOT EXISTS dim_commodity (
    id_commodity    SERIAL          PRIMARY KEY,
    codigo_sidra    VARCHAR(10)     NOT NULL UNIQUE,
    nome_oficial    VARCHAR(100)    NOT NULL,
    unidade_medida  VARCHAR(30)     NOT NULL DEFAULT 'Toneladas',

    -- Justificativa das chaves:
    -- PK: id_commodity (SERIAL) → surrogate key, evita dependência do
    --     código SIDRA externo que pode mudar.
    -- UNIQUE codigo_sidra → garante que cada produto IBGE apareça
    --     uma única vez, usado como lookup na carga incremental.

    CONSTRAINT chk_codigo_sidra CHECK (codigo_sidra ~ '^[0-9]+$')
);
"""

DDL_DIM_TERRITORIO = """
CREATE TABLE IF NOT EXISTS dim_territorio (
    id_territorio       SERIAL          PRIMARY KEY,
    codigo_ibge         VARCHAR(10)     NOT NULL,
    nome                VARCHAR(100)    NOT NULL,
    nivel_territorial   VARCHAR(30)     NOT NULL
                            CHECK (nivel_territorial IN ('Brasil', 'Unidade da Federação', 'Município')),

    -- Justificativa das chaves:
    -- PK: id_territorio (SERIAL) → surrogate key independente do
    --     código IBGE, que pode ter leading zeros e variar por nível.
    -- UNIQUE (codigo_ibge, nivel_territorial) → um mesmo código pode
    --     existir em níveis diferentes (ex: código "1" = Brasil no n1,
    --     mas código "1" também é um UF no n3). A combinação é única.

    CONSTRAINT uq_territorio UNIQUE (codigo_ibge, nivel_territorial)
);
"""

DDL_FATO_PRODUCAO = """
CREATE TABLE IF NOT EXISTS fato_producao (
    id_producao             SERIAL          PRIMARY KEY,
    id_commodity            INTEGER         NOT NULL
                                REFERENCES dim_commodity(id_commodity)
                                ON DELETE RESTRICT,
    id_territorio           INTEGER         NOT NULL
                                REFERENCES dim_territorio(id_territorio)
                                ON DELETE RESTRICT,
    ano                     SMALLINT        NOT NULL
                                CHECK (ano BETWEEN 1974 AND 2100),
    area_colhida_ha         NUMERIC(15, 2),
    qtd_produzida           NUMERIC(18, 2),
    rendimento_medio_kg_ha  NUMERIC(10, 2),
    valor_producao_mil_reais NUMERIC(18, 2),
    fonte                   VARCHAR(30)     NOT NULL DEFAULT 'IBGE/PAM-SIDRA',
    tabela_sidra            VARCHAR(10)     NOT NULL DEFAULT '5457',
    url_origem              TEXT,
    coletado_em             TIMESTAMP       NOT NULL DEFAULT NOW(),

    -- Justificativa das chaves:
    -- PK: id_producao (SERIAL) → surrogate key para facilitar
    --     referências em outras tabelas futuras.
    -- FK id_commodity → dim_commodity: garante integridade referencial,
    --     impede inserção de produção para commodity inexistente.
    -- FK id_territorio → dim_territorio: idem para território.
    -- UNIQUE (id_commodity, id_territorio, ano) → impede duplicidade
    --     de medição para a mesma combinação produto × local × ano.
    --     Essencial para carga incremental (UPSERT).
    -- ON DELETE RESTRICT → protege histórico de produção contra
    --     exclusão acidental de dimensões referenciadas.

    CONSTRAINT uq_producao UNIQUE (id_commodity, id_territorio, ano)
);
"""

DDL_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_fato_ano
    ON fato_producao (ano);

CREATE INDEX IF NOT EXISTS idx_fato_commodity
    ON fato_producao (id_commodity);

CREATE INDEX IF NOT EXISTS idx_fato_territorio
    ON fato_producao (id_territorio);

CREATE INDEX IF NOT EXISTS idx_fato_ano_commodity
    ON fato_producao (ano, id_commodity);
"""


# ──────────────────────────────────────────────────────────────
#  CONEXÃO
# ──────────────────────────────────────────────────────────────

def conectar() -> psycopg2.extensions.connection:
    """
    Estabelece conexão com o PostgreSQL usando as variáveis do .env.

    Returns:
        Conexão psycopg2 ativa.

    Raises:
        SystemExit: se a conexão falhar.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        log.info(
            "Conectado ao PostgreSQL: %s:%s/%s",
            DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"],
        )
        return conn
    except psycopg2.OperationalError as exc:
        log.error("Falha na conexão com PostgreSQL: %s", exc)
        log.error("Verifique as variáveis DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD no .env")
        sys.exit(1)


# ──────────────────────────────────────────────────────────────
#  DDL — CRIAÇÃO DAS TABELAS
# ──────────────────────────────────────────────────────────────

def criar_tabelas(conn: psycopg2.extensions.connection) -> None:
    """
    Cria as tabelas dimensão e fato no PostgreSQL se não existirem.

    Usa IF NOT EXISTS em todas as DDLs para ser idempotente
    (pode rodar múltiplas vezes sem erro).

    Args:
        conn: Conexão PostgreSQL ativa.
    """
    with conn.cursor() as cur:
        log.info("Criando tabelas (IF NOT EXISTS)...")

        cur.execute(DDL_DIM_COMMODITY)
        log.info("  dim_commodity       ✔")

        cur.execute(DDL_DIM_TERRITORIO)
        log.info("  dim_territorio      ✔")

        cur.execute(DDL_FATO_PRODUCAO)
        log.info("  fato_producao       ✔")

        cur.execute(DDL_INDEXES)
        log.info("  indexes             ✔")

    conn.commit()


# ──────────────────────────────────────────────────────────────
#  LEITURA DO CSV
# ──────────────────────────────────────────────────────────────

def ler_csv() -> pd.DataFrame:
    """
    Lê o CSV mais recente de data/raw/csv/.

    Returns:
        DataFrame com os dados brutos.

    Raises:
        SystemExit: se nenhum CSV for encontrado.
    """
    csvs = sorted(PASTA_CSV.glob("ibge_pam_*.csv"), reverse=True)
    if not csvs:
        log.error("Nenhum CSV encontrado em %s.", PASTA_CSV)
        log.error("Execute primeiro: python main.py")
        sys.exit(1)

    arquivo = csvs[0]
    log.info("Lendo: %s", arquivo.name)
    df = pd.read_csv(arquivo, encoding="utf-8-sig")
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
    log.info("  %d linhas × %d colunas", len(df), len(df.columns))
    return df


# ──────────────────────────────────────────────────────────────
#  CARGA — DIMENSÕES
# ──────────────────────────────────────────────────────────────

def carregar_dim_commodity(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame,
) -> dict[str, int]:
    """
    Insere os produtos únicos na dim_commodity.

    Usa INSERT ... ON CONFLICT DO NOTHING para ser idempotente:
    re-execuções não duplicam registros.

    Args:
        conn: Conexão PostgreSQL ativa.
        df:   DataFrame com os dados brutos.

    Returns:
        Dict {codigo_sidra: id_commodity} para uso na carga da fato.
    """
    commodities = (
        df[["produto_cod", "produto", "unidade_qtd"]]
        .drop_duplicates("produto_cod")
        .rename(columns={
            "produto_cod" : "codigo_sidra",
            "produto"     : "nome_oficial",
            "unidade_qtd" : "unidade_medida",
        })
    )

    rows = [
        (
            str(row["codigo_sidra"]),
            str(row["nome_oficial"]),
            str(row["unidade_medida"]) if pd.notna(row["unidade_medida"]) else "Toneladas",
        )
        for _, row in commodities.iterrows()
    ]

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO dim_commodity (codigo_sidra, nome_oficial, unidade_medida)
            VALUES %s
            ON CONFLICT (codigo_sidra) DO NOTHING
            """,
            rows,
        )

        # Busca IDs gerados para montar o mapeamento
        cur.execute("SELECT codigo_sidra, id_commodity FROM dim_commodity")
        mapa = {row[0]: row[1] for row in cur.fetchall()}

    conn.commit()
    log.info("dim_commodity: %d produtos inseridos/verificados.", len(mapa))
    return mapa


def carregar_dim_territorio(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame,
) -> dict[tuple, int]:
    """
    Insere os territórios únicos na dim_territorio.

    A chave de lookup é (codigo_ibge, nivel_territorial) — necessária
    porque o mesmo código pode existir em diferentes níveis territoriais.

    Args:
        conn: Conexão PostgreSQL ativa.
        df:   DataFrame com os dados brutos.

    Returns:
        Dict {(codigo_ibge, nivel_territorial): id_territorio}.
    """
    territorios = (
        df[["cod_territorial", "nome_territorial", "nivel_territorial"]]
        .drop_duplicates()
        .rename(columns={
            "cod_territorial"  : "codigo_ibge",
            "nome_territorial" : "nome",
        })
    )

    rows = [
        (
            str(row["codigo_ibge"]),
            str(row["nome"]),
            str(row["nivel_territorial"]),
        )
        for _, row in territorios.iterrows()
    ]

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO dim_territorio (codigo_ibge, nome, nivel_territorial)
            VALUES %s
            ON CONFLICT (codigo_ibge, nivel_territorial) DO NOTHING
            """,
            rows,
        )

        cur.execute(
            "SELECT codigo_ibge, nivel_territorial, id_territorio FROM dim_territorio"
        )
        mapa = {(row[0], row[1]): row[2] for row in cur.fetchall()}

    conn.commit()
    log.info("dim_territorio: %d territórios inseridos/verificados.", len(mapa))
    return mapa


# ──────────────────────────────────────────────────────────────
#  CARGA — FATO
# ──────────────────────────────────────────────────────────────

def carregar_fato_producao(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame,
    mapa_commodity: dict[str, int],
    mapa_territorio: dict[tuple, int],
) -> int:
    """
    Insere os registros de produção na fato_producao.

    Usa INSERT ... ON CONFLICT DO UPDATE (UPSERT) para permitir
    reprocessamento sem duplicar linhas: se a combinação
    (id_commodity, id_territorio, ano) já existe, atualiza os valores.

    Args:
        conn:             Conexão PostgreSQL ativa.
        df:               DataFrame com os dados brutos.
        mapa_commodity:   {codigo_sidra: id_commodity}
        mapa_territorio:  {(codigo_ibge, nivel): id_territorio}

    Returns:
        Número de linhas inseridas ou atualizadas.
    """
    rows = []
    ignoradas = 0

    for _, row in df.iterrows():
        id_commodity  = mapa_commodity.get(str(row["produto_cod"]))
        id_territorio = mapa_territorio.get(
            (str(row["cod_territorial"]), str(row["nivel_territorial"]))
        )

        if not id_commodity or not id_territorio:
            log.warning(
                "Linha ignorada — FK não encontrada: produto_cod=%s, cod_territorial=%s",
                row["produto_cod"], row["cod_territorial"],
            )
            ignoradas += 1
            continue

        def safe(val):
            """Converte NaN/NA para None (NULL no PostgreSQL)."""
            return None if pd.isna(val) else float(val)

        rows.append((
            id_commodity,
            id_territorio,
            int(row["ano"]),
            safe(row.get("area_colhida_ha")),
            safe(row.get("qtd_produzida")),
            safe(row.get("rendimento_medio_kg_ha")),
            safe(row.get("valor_producao_mil_reais")),
            str(row.get("fonte", "IBGE/PAM-SIDRA")),
            str(row.get("tabela_sidra", "5457")),
            str(row.get("url_origem", "")),
            str(row.get("coletado_em", "")),
        ))

    if not rows:
        log.warning("Nenhuma linha válida para inserir na fato_producao.")
        return 0

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO fato_producao (
                id_commodity, id_territorio, ano,
                area_colhida_ha, qtd_produzida,
                rendimento_medio_kg_ha, valor_producao_mil_reais,
                fonte, tabela_sidra, url_origem, coletado_em
            ) VALUES %s
            ON CONFLICT (id_commodity, id_territorio, ano)
            DO UPDATE SET
                area_colhida_ha          = EXCLUDED.area_colhida_ha,
                qtd_produzida            = EXCLUDED.qtd_produzida,
                rendimento_medio_kg_ha   = EXCLUDED.rendimento_medio_kg_ha,
                valor_producao_mil_reais = EXCLUDED.valor_producao_mil_reais,
                coletado_em              = EXCLUDED.coletado_em
            """,
            rows,
        )

    conn.commit()
    log.info(
        "fato_producao: %d linhas inseridas/atualizadas, %d ignoradas.",
        len(rows), ignoradas,
    )
    return len(rows)


# ──────────────────────────────────────────────────────────────
#  VERIFICAÇÃO
# ──────────────────────────────────────────────────────────────

def verificar_carga(conn: psycopg2.extensions.connection) -> None:
    """
    Executa queries de verificação após a carga e exibe os resultados.

    Verifica:
        - Contagem por tabela
        - Top 5 commodities por valor de produção (2023)
        - Integridade referencial (FKs órfãs)

    Args:
        conn: Conexão PostgreSQL ativa.
    """
    with conn.cursor() as cur:

        # Contagem por tabela
        log.info("─" * 50)
        log.info("Verificação pós-carga:")
        for tabela in ["dim_commodity", "dim_territorio", "fato_producao"]:
            cur.execute(f"SELECT COUNT(*) FROM {tabela}")
            n = cur.fetchone()[0]
            log.info("  %-25s %d linhas", tabela, n)

        # Top 5 por valor de produção
        cur.execute("""
            SELECT
                c.nome_oficial,
                f.ano,
                f.qtd_produzida,
                f.valor_producao_mil_reais
            FROM fato_producao f
            JOIN dim_commodity  c ON c.id_commodity  = f.id_commodity
            JOIN dim_territorio t ON t.id_territorio = f.id_territorio
            WHERE f.ano = (SELECT MAX(ano) FROM fato_producao WHERE qtd_produzida IS NOT NULL)
              AND f.valor_producao_mil_reais IS NOT NULL
            ORDER BY f.valor_producao_mil_reais DESC
            LIMIT 5
        """)
        rows = cur.fetchall()
        log.info("─" * 50)
        log.info("Top 5 por valor de produção (ano mais recente com dados):")
        log.info("  %-35s %6s  %18s  %20s",
                 "Produto", "Ano", "Qtd (ton)", "Valor (Mil R$)")
        log.info("  " + "-" * 80)
        for row in rows:
            nome = str(row[0])[:34]
            ano  = str(row[1])
            qtd  = f"{row[2]:>18,.0f}" if row[2] else f"{'—':>18}"
            val  = f"{row[3]:>20,.0f}" if row[3] else f"{'—':>20}"
            log.info("  %-35s %6s  %s  %s", nome, ano, qtd, val)

        # Verifica FKs órfãs
        cur.execute("""
            SELECT COUNT(*) FROM fato_producao
            WHERE id_commodity NOT IN (SELECT id_commodity FROM dim_commodity)
               OR id_territorio NOT IN (SELECT id_territorio FROM dim_territorio)
        """)
        orfas = cur.fetchone()[0]
        log.info("─" * 50)
        log.info("FKs órfãs: %d %s", orfas, "(OK)" if orfas == 0 else "(PROBLEMA!)")


# ──────────────────────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────────────────────

def main() -> None:
    """
    Orquestra a carga dos dados no PostgreSQL.

    Sequência:
        1. Conecta ao PostgreSQL
        2. Cria tabelas (DDL idempotente)
        3. Lê o CSV da camada Raw
        4. Carrega dim_commodity
        5. Carrega dim_territorio
        6. Carrega fato_producao (UPSERT)
        7. Verifica integridade pós-carga
    """
    log.info("=" * 55)
    log.info("PostgreSQL Load — agro-data-pipeline")
    log.info("=" * 55)

    conn = conectar()

    try:
        # 1. DDL
        criar_tabelas(conn)

        # 2. Lê CSV
        df = ler_csv()

        # 3. Dimensões
        mapa_commodity  = carregar_dim_commodity(conn, df)
        mapa_territorio = carregar_dim_territorio(conn, df)

        # 4. Fato
        carregar_fato_producao(conn, df, mapa_commodity, mapa_territorio)

        # 5. Verificação
        verificar_carga(conn)

        log.info("=" * 55)
        log.info("Carga concluída com sucesso.")
        log.info("=" * 55)

    except Exception as exc:
        conn.rollback()
        log.error("Erro durante a carga: %s", exc)
        raise

    finally:
        conn.close()
        log.info("Conexão encerrada.")


if __name__ == "__main__":
    main()