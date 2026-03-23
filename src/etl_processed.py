"""
etl_processed.py
================
ETL — Extração, Transformação e Carga na camada Processed.

Lê os dados brutos da camada Raw (data/raw/csv/),
aplica todas as transformações necessárias e:
    1. Salva localmente em data/processed/ (CSV)
    2. Carrega no PostgreSQL (schema: processed)

Transformações aplicadas:
    1. Correção de tipos de dados
       - coletado_em  : str  → timestamp
       - produto_cod  : int  → str (código não é número)
       - tabela_sidra : int  → str
       - ano          : int  → smallint
       - numéricas    : float64 com precisão controlada

    2. Tratamento de valores ausentes
       - area_colhida_ha, qtd_produzida, rendimento_medio_kg_ha:
         NaN = dado não publicado pelo IBGE (mantido como NULL)
       - valor_producao_mil_reais:
         NaN = dado não publicado (mantido como NULL)
       - Coluna 'status_dado' indica origem do NULL:
         'completo' | 'parcial'

    3. Padronização de categorias
       - produto           : strip + title case + mapa de normalização
       - nivel_territorial : strip + title case
       - unidade_qtd       : strip + upper case
       - fonte             : strip + upper case

    4. Validações
       - ano entre 1974 e ano atual
       - produto_cod numérico
       - valores numéricos não negativos

Saídas:
    data/processed/
        └── ibge_pam_processed_YYYYMMDD.csv   ← arquivo local
    PostgreSQL: processed.producao_agricola   ← banco de dados

Execução:
    python src/etl_processed.py
"""

import logging
import os
import sys
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()


# ──────────────────────────────────────────────────────────────
#  CONFIGURAÇÃO
# ──────────────────────────────────────────────────────────────

PASTA_CSV       = Path(os.getenv("OUTPUT_DIR", "data/raw")) / "csv"
PASTA_PROCESSED = Path("data/processed")
SCHEMA          = "processed"
ANO_MINIMO      = 1974
ANO_MAXIMO      = date.today().year

DB_CONFIG = {
    "host"    : os.getenv("DB_HOST",     "localhost"),
    "port"    : int(os.getenv("DB_PORT", "5432")),
    "dbname"  : os.getenv("DB_NAME",     "agro_dw"),
    "user"    : os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# Mapa de normalização de nomes de produtos
MAPA_PRODUTO = {
    "soja (em grão)"              : "Soja",
    "milho (em grão)"             : "Milho",
    "café (em grão) total"        : "Café",
    "cafe (em grao) total"        : "Café",
    "cana-de-açúcar"              : "Cana-de-Açúcar",
    "cana de acucar"              : "Cana-de-Açúcar",
    "algodão herbáceo (em caroço)": "Algodão",
    "algodao herbaceo (em caroco)": "Algodão",
    "trigo (em grão)"             : "Trigo",
    "arroz (em casca)"            : "Arroz",
    "feijão (em grão)"            : "Feijão",
    "feijao (em grao)"            : "Feijão",
    "soja"                        : "Soja",
    "milho"                       : "Milho",
    "café"                        : "Café",
    "cafe"                        : "Café",
    "algodão"                     : "Algodão",
    "algodao"                     : "Algodão",
    "trigo"                       : "Trigo",
    "arroz"                       : "Arroz",
    "feijão"                      : "Feijão",
    "feijao"                      : "Feijão",
    "mandioca"                    : "Mandioca",
    "sorgo (em grão)"             : "Sorgo",
    "girassol (em grão)"          : "Girassol",
    "aveia (em grão)"             : "Aveia",
    "cevada (em grão)"            : "Cevada",
    "triticale (em grão)"         : "Triticale",
    "laranja"                     : "Laranja",
    "banana (cacho)"              : "Banana",
    "cacau (em amêndoa)"          : "Cacau",
    "borracha (látex coagulado)"  : "Borracha",
    "sisal ou agave (fibra)"      : "Sisal",
}


# ──────────────────────────────────────────────────────────────
#  LOGGING
# ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_processed.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("etl")


# ──────────────────────────────────────────────────────────────
#  EXTRAÇÃO
# ──────────────────────────────────────────────────────────────

def extrair() -> pd.DataFrame:
    """
    Lê o CSV mais recente da camada Raw.

    Returns:
        DataFrame bruto sem nenhuma transformação.

    Raises:
        SystemExit: se nenhum CSV for encontrado.
    """
    csvs = sorted(PASTA_CSV.glob("ibge_pam_*.csv"), reverse=True)
    if not csvs:
        log.error("Nenhum CSV encontrado em %s.", PASTA_CSV)
        log.error("Execute primeiro: python main.py")
        sys.exit(1)

    arquivo = csvs[0]
    log.info("Extraindo: %s", arquivo.name)
    df = pd.read_csv(arquivo, encoding="utf-8-sig")
    log.info("  Shape bruto: %d linhas × %d colunas", len(df), len(df.columns))
    return df


# ──────────────────────────────────────────────────────────────
#  TRANSFORMAÇÃO
# ──────────────────────────────────────────────────────────────

def corrigir_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Corrige os tipos de dado de cada coluna.

    Args:
        df: DataFrame bruto.

    Returns:
        DataFrame com tipos corrigidos.
    """
    log.info("  [1/4] Corrigindo tipos de dados...")

    df["produto_cod"]     = df["produto_cod"].astype(str).str.strip()
    df["tabela_sidra"]    = df["tabela_sidra"].astype(str).str.strip()
    df["cod_territorial"] = df["cod_territorial"].astype(str).str.strip()
    df["ano"]             = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
    df["coletado_em"]     = pd.to_datetime(df["coletado_em"], errors="coerce")

    for col in ["area_colhida_ha", "qtd_produzida",
                "rendimento_medio_kg_ha", "valor_producao_mil_reais"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    log.info("    produto_cod   : int64  → str")
    log.info("    tabela_sidra  : int64  → str")
    log.info("    coletado_em   : str    → datetime")
    log.info("    numéricas     : float64 com 2 casas decimais")
    return df


def tratar_ausentes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trata valores ausentes e adiciona coluna de status do dado.

    Args:
        df: DataFrame com tipos corrigidos.

    Returns:
        DataFrame com coluna status_dado adicionada.
    """
    log.info("  [2/4] Tratando valores ausentes...")

    colunas_metrica = [
        "area_colhida_ha", "qtd_produzida",
        "rendimento_medio_kg_ha", "valor_producao_mil_reais",
    ]

    for col in colunas_metrica:
        nulos = df[col].isnull().sum()
        if nulos > 0:
            log.info("    %-30s %d NaN → mantido como NULL", col, nulos)

    tem_nulo = df[colunas_metrica].isnull().any(axis=1)
    df["status_dado"] = "completo"
    df.loc[tem_nulo, "status_dado"] = "parcial"

    log.info(
        "    status_dado: %d completos, %d parciais",
        (~tem_nulo).sum(), tem_nulo.sum(),
    )
    return df


def padronizar_categorias(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza colunas categóricas para garantir consistência.

    Args:
        df: DataFrame com ausentes tratados.

    Returns:
        DataFrame com categorias padronizadas.
    """
    log.info("  [3/4] Padronizando categorias...")

    df["produto_nome_ibge"] = df["produto"].str.strip()

    df["produto"] = (
        df["produto"]
        .str.strip()
        .str.lower()
        .map(MAPA_PRODUTO)
        .fillna(df["produto"].str.strip().str.title())
    )

    df["nivel_territorial"] = df["nivel_territorial"].str.strip().str.title()
    df["nome_territorial"]  = df["nome_territorial"].str.strip().str.title()
    df["unidade_qtd"]       = df["unidade_qtd"].str.strip().str.upper()
    df["fonte"]             = df["fonte"].str.strip().str.upper()

    log.info("    produto           : normalizado via mapa canônico")
    log.info("    produto_nome_ibge : nome original preservado")
    log.info("    nivel_territorial : title case")
    log.info("    unidade_qtd       : upper case")
    log.info("    fonte             : upper case")

    log.info("    Produtos após padronização:")
    for nome in sorted(df["produto"].unique()):
        log.info("      → %s", nome)

    return df


def validar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Valida regras de negócio e remove registros inválidos.

    Args:
        df: DataFrame padronizado.

    Returns:
        DataFrame apenas com registros válidos.
    """
    log.info("  [4/4] Validando regras de negócio...")
    total_antes = len(df)

    mask_ano = df["ano"].between(ANO_MINIMO, ANO_MAXIMO)
    if (~mask_ano).sum():
        log.warning("    %d registros com ano inválido removidos.", (~mask_ano).sum())
    df = df[mask_ano]

    mask_cod = df["produto_cod"].str.isnumeric()
    if (~mask_cod).sum():
        log.warning("    %d registros com produto_cod inválido removidos.", (~mask_cod).sum())
    df = df[mask_cod]

    for col in ["area_colhida_ha", "qtd_produzida",
                "rendimento_medio_kg_ha", "valor_producao_mil_reais"]:
        mask_neg = df[col].notna() & (df[col] < 0)
        if mask_neg.sum():
            log.warning("    %d valores negativos em '%s' → convertidos para NULL.", mask_neg.sum(), col)
            df.loc[mask_neg, col] = None

    log.info("    %d/%d registros aprovados.", len(df), total_antes)
    return df.reset_index(drop=True)


def adicionar_metadados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona colunas de controle do pipeline.

    Args:
        df: DataFrame validado.

    Returns:
        DataFrame com metadados de pipeline.
    """
    df["processado_em"] = datetime.now()
    df["versao_etl"]    = "1.0"
    return df


def transformar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Orquestra todas as etapas de transformação.

    Args:
        df: DataFrame bruto da extração.

    Returns:
        DataFrame limpo, padronizado e validado.
    """
    log.info("Iniciando transformações...")
    df = corrigir_tipos(df)
    df = tratar_ausentes(df)
    df = padronizar_categorias(df)
    df = validar(df)
    df = adicionar_metadados(df)
    log.info("Transformações concluídas: %d linhas prontas para carga.", len(df))
    return df


# ──────────────────────────────────────────────────────────────
#  SALVAMENTO LOCAL — data/processed/
# ──────────────────────────────────────────────────────────────

def salvar_local(df: pd.DataFrame) -> Path:
    """
    Salva o DataFrame tratado em CSV na pasta data/processed/.

    Nome do arquivo:
        ibge_pam_processed_YYYYMMDD.csv

    Encoding: UTF-8 com BOM para compatibilidade com Excel.

    Args:
        df: DataFrame transformado.

    Returns:
        Path do arquivo salvo.
    """
    PASTA_PROCESSED.mkdir(parents=True, exist_ok=True)

    hoje  = date.today().strftime("%Y%m%d")
    nome  = f"ibge_pam_processed_{hoje}.csv"
    arq   = PASTA_PROCESSED / nome

    df.to_csv(arq, index=False, encoding="utf-8-sig")

    log.info("CSV local → %s  (%.1f KB)", arq, arq.stat().st_size / 1024)
    return arq


# ──────────────────────────────────────────────────────────────
#  CARGA — POSTGRESQL
# ──────────────────────────────────────────────────────────────

DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS processed;"

DDL_PROCESSED = """
CREATE TABLE IF NOT EXISTS processed.producao_agricola (
    id                       SERIAL          PRIMARY KEY,
    produto                  VARCHAR(50)     NOT NULL,
    produto_nome_ibge        VARCHAR(150)    NOT NULL,
    produto_cod              VARCHAR(10)     NOT NULL,
    nivel_territorial        VARCHAR(30)     NOT NULL,
    cod_territorial          VARCHAR(10)     NOT NULL,
    nome_territorial         VARCHAR(100)    NOT NULL,
    ano                      SMALLINT        NOT NULL
                                 CHECK (ano BETWEEN 1974 AND 2100),
    area_colhida_ha          NUMERIC(15, 2),
    qtd_produzida            NUMERIC(18, 2),
    unidade_qtd              VARCHAR(20)     NOT NULL,
    rendimento_medio_kg_ha   NUMERIC(10, 2),
    valor_producao_mil_reais NUMERIC(18, 2),
    status_dado              VARCHAR(15)     NOT NULL
                                 DEFAULT 'completo'
                                 CHECK (status_dado IN ('completo', 'parcial')),
    fonte                    VARCHAR(30)     NOT NULL,
    tabela_sidra             VARCHAR(10)     NOT NULL,
    url_origem               TEXT,
    coletado_em              TIMESTAMP,
    processado_em            TIMESTAMP       NOT NULL DEFAULT NOW(),
    versao_etl               VARCHAR(10)     NOT NULL DEFAULT '1.0',

    CONSTRAINT uq_producao_processed
        UNIQUE (produto_cod, cod_territorial, nivel_territorial, ano)
);

CREATE INDEX IF NOT EXISTS idx_proc_produto
    ON processed.producao_agricola (produto);

CREATE INDEX IF NOT EXISTS idx_proc_ano
    ON processed.producao_agricola (ano);

CREATE INDEX IF NOT EXISTS idx_proc_produto_ano
    ON processed.producao_agricola (produto, ano);
"""

COLUNAS_INSERT = [
    "produto", "produto_nome_ibge", "produto_cod",
    "nivel_territorial", "cod_territorial", "nome_territorial",
    "ano",
    "area_colhida_ha", "qtd_produzida", "unidade_qtd",
    "rendimento_medio_kg_ha", "valor_producao_mil_reais",
    "status_dado", "fonte", "tabela_sidra",
    "url_origem", "coletado_em", "processado_em", "versao_etl",
]


def conectar() -> psycopg2.extensions.connection:
    """
    Conecta ao PostgreSQL usando variáveis do .env.

    Returns:
        Conexão psycopg2 ativa.

    Raises:
        SystemExit: se a conexão falhar.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        log.info(
            "Conectado: %s:%s/%s",
            DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"],
        )
        return conn
    except psycopg2.OperationalError as exc:
        log.error("Falha na conexão: %s", exc)
        sys.exit(1)


def criar_schema_processed(conn: psycopg2.extensions.connection) -> None:
    """
    Cria o schema 'processed' e a tabela se não existirem.

    Args:
        conn: Conexão PostgreSQL ativa.
    """
    with conn.cursor() as cur:
        cur.execute(DDL_SCHEMA)
        cur.execute(DDL_PROCESSED)
    conn.commit()
    log.info("Schema e tabela processed verificados.")


def carregar_processed(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame,
) -> int:
    """
    Insere os dados tratados na tabela processed.producao_agricola.

    Usa UPSERT para garantir idempotência.

    Args:
        conn: Conexão PostgreSQL ativa.
        df:   DataFrame transformado.

    Returns:
        Número de linhas inseridas ou atualizadas.
    """
    def safe(val):
        """Converte NaN/NaT para None (NULL no PostgreSQL)."""
        if val is None:
            return None
        try:
            if pd.isna(val):
                return None
        except (TypeError, ValueError):
            pass
        return val

    rows = [
        tuple(safe(row[col]) for col in COLUNAS_INSERT)
        for _, row in df.iterrows()
    ]

    sql = f"""
        INSERT INTO processed.producao_agricola
            ({', '.join(COLUNAS_INSERT)})
        VALUES %s
        ON CONFLICT (produto_cod, cod_territorial, nivel_territorial, ano)
        DO UPDATE SET
            produto                  = EXCLUDED.produto,
            produto_nome_ibge        = EXCLUDED.produto_nome_ibge,
            area_colhida_ha          = EXCLUDED.area_colhida_ha,
            qtd_produzida            = EXCLUDED.qtd_produzida,
            rendimento_medio_kg_ha   = EXCLUDED.rendimento_medio_kg_ha,
            valor_producao_mil_reais = EXCLUDED.valor_producao_mil_reais,
            status_dado              = EXCLUDED.status_dado,
            processado_em            = EXCLUDED.processado_em,
            versao_etl               = EXCLUDED.versao_etl
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)

    conn.commit()
    log.info("processed.producao_agricola: %d linhas inseridas/atualizadas.", len(rows))
    return len(rows)


def verificar(conn: psycopg2.extensions.connection) -> None:
    """
    Exibe relatório de qualidade após a carga.

    Args:
        conn: Conexão PostgreSQL ativa.
    """
    with conn.cursor() as cur:
        log.info("─" * 55)
        log.info("Verificação pós-carga")
        log.info("─" * 55)

        cur.execute("SELECT COUNT(*) FROM processed.producao_agricola")
        log.info("  Total de registros : %d", cur.fetchone()[0])

        cur.execute("""
            SELECT status_dado, COUNT(*) AS total
            FROM processed.producao_agricola
            GROUP BY status_dado ORDER BY status_dado
        """)
        log.info("  Por status_dado:")
        for row in cur.fetchall():
            log.info("    %-12s → %d", row[0], row[1])

        log.info("─" * 55)


# ──────────────────────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────────────────────

def main() -> None:
    """
    Orquestra o pipeline ETL completo.

    Sequência:
        E — extrair()         lê CSV da camada Raw
        T — transformar()     aplica 4 etapas de limpeza
        L — salvar_local()    salva em data/processed/
            + carregar_processed()  grava no PostgreSQL
        V — verificar()       relatório de qualidade
    """
    log.info("=" * 55)
    log.info("ETL — Camada Processed | agro-data-pipeline")
    log.info("=" * 55)

    # E — Extração
    log.info("[E] Extração")
    df_raw = extrair()

    # T — Transformação
    log.info("[T] Transformação")
    df_proc = transformar(df_raw)

    # L — Carga local + PostgreSQL
    log.info("[L] Salvando localmente em data/processed/")
    arq_local = salvar_local(df_proc)

    log.info("[L] Carga no PostgreSQL")
    conn = conectar()
    try:
        criar_schema_processed(conn)
        carregar_processed(conn, df_proc)
        verificar(conn)
    except Exception as exc:
        conn.rollback()
        log.error("Erro na carga: %s", exc)
        raise
    finally:
        conn.close()
        log.info("Conexão encerrada.")

    log.info("=" * 55)
    log.info("ETL concluído com sucesso.")
    log.info("  CSV local → %s", arq_local)
    log.info("  PostgreSQL → processed.producao_agricola")
    log.info("=" * 55)


if __name__ == "__main__":
    main()