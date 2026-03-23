"""
main.py
=======
Ponto de entrada único do pipeline agro-data-pipeline.

Orquestra todas as etapas em sequência:
    1. Configuração  — carrega .env e inicializa logging
    2. Coleta        — busca dados na API IBGE/SIDRA (PAM Tabela 5457)
    3. Camada Raw    — persiste dados brutos em CSV, JSON e Parquet
    4. Manifesto     — registra metadados dos arquivos gerados
    5. Sumário       — exibe relatório final no terminal

Execução:
    python main.py
"""

import logging
import sys
from datetime import datetime

from src.gerar_parquet import (
    ANOS,
    NIVEL,
    NIVEIS,
    PASTA_SAIDA,
    TABELA,
    coletar,
    configurar_logging,
    salvar_csv,
    salvar_json,
    salvar_manifesto,
    salvar_parquet,
    _nome_base,
)


# ──────────────────────────────────────────────────────────────
#  LOGGING
# ──────────────────────────────────────────────────────────────

log = configurar_logging()


# ──────────────────────────────────────────────────────────────
#  PIPELINE
# ──────────────────────────────────────────────────────────────

def executar_pipeline() -> None:
    """
    Orquestra todas as etapas do pipeline de dados agrícolas.

    Etapas:
        1. coletar()          — requisições à API IBGE/SIDRA com retry
        2. salvar_csv()       — dados brutos em data/raw/csv/
        3. salvar_json()      — dados brutos em data/raw/json/
        4. salvar_parquet()   — dados brutos em data/raw/parquet/ (Snappy)
        5. salvar_manifesto() — metadados em data/raw/_manifesto.json

    Raises:
        SystemExit: se a coleta não retornar nenhum dado.
    """
    inicio = datetime.now()

    log.info("=" * 55)
    log.info("AGRO DATA PIPELINE — IBGE/SIDRA PAM Tabela %s", TABELA)
    log.info("=" * 55)
    log.info("Anos    : %s", ANOS)
    log.info("Nível   : %s (%s)", NIVEL, NIVEIS[NIVEL])
    log.info("Saída   : %s", PASTA_SAIDA.resolve())
    log.info("=" * 55)

    # ── Etapa 1: Coleta ──────────────────────────────────────
    log.info("[1/4] Coletando dados da API IBGE/SIDRA...")
    df = coletar()

    if df.empty:
        log.error("Nenhum dado coletado. Verifique a conexão com a internet.")
        sys.exit(1)

    log.info("      %d linhas × %d colunas coletadas.", len(df), len(df.columns))

    # ── Etapa 2: Camada Raw ───────────────────────────────────
    log.info("[2/4] Salvando camada Raw...")
    nome = _nome_base()

    arq_csv     = salvar_csv(df, nome)
    arq_json    = salvar_json(df, nome)
    arq_parquet = salvar_parquet(df, nome)

    # ── Etapa 3: Manifesto ────────────────────────────────────
    log.info("[3/4] Gravando manifesto...")
    salvar_manifesto({
        "csv"    : arq_csv,
        "json"   : arq_json,
        "parquet": arq_parquet,
    })

    # ── Etapa 4: Sumário ──────────────────────────────────────
    log.info("[4/4] Sumário da execução:")
    duracao = (datetime.now() - inicio).seconds
    log.info("=" * 55)
    log.info("  Produtos coletados : %d", df["produto"].nunique())
    log.info("  Anos               : %s", sorted(df["ano"].dropna().unique().tolist()))
    log.info("  Total de registros : %d", len(df))
    log.info("  Arquivos gerados   :")
    log.info("    CSV     → %s", arq_csv)
    log.info("    JSON    → %s", arq_json)
    if arq_parquet:
        log.info("    Parquet → %s", arq_parquet)
    log.info("  Duração            : %ds", duracao)
    log.info("=" * 55)
    log.info("Pipeline concluído com sucesso.")


# ──────────────────────────────────────────────────────────────
#  ENTRY POINT
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    executar_pipeline()