"""
raw_layer.py
============
Organiza a camada Raw do pipeline de dados agrícolas.

Responsabilidades:
    - Salvar os dados brutos nos 3 formatos: CSV, JSON e Parquet
    - Estruturar os diretórios por fonte / nível / ano
    - Gerar um manifesto JSON com metadados de cada arquivo salvo
    - Nunca transformar os dados — camada raw preserva o original

Estrutura gerada em data/raw/:
    data/raw/
    ├── csv/
    │   └── ibge_pam_brasil_2023_2024_YYYYMMDD.csv
    ├── json/
    │   └── ibge_pam_brasil_2023_2024_YYYYMMDD.json
    ├── parquet/
    │   └── ibge_pam_brasil_2023_2024_YYYYMMDD.parquet
    └── _manifesto.json

Execução (chamado automaticamente por teste.py):
    python src/raw_layer.py
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

import pandas as pd


log = logging.getLogger("sidra.raw")


# ──────────────────────────────────────────────────────────────
#  CONSTANTES
# ──────────────────────────────────────────────────────────────

FORMATOS_SUPORTADOS = ("csv", "json", "parquet")


# ──────────────────────────────────────────────────────────────
#  FUNÇÕES DE SALVAMENTO
# ──────────────────────────────────────────────────────────────

def salvar_csv(df: pd.DataFrame, destino: Path) -> dict:
    """
    Salva o DataFrame em formato CSV.

    Vantagens do CSV:
        + Universal — abre em Excel, LibreOffice, pandas, R, qualquer editor
        + Legível por humanos sem ferramenta especial
        + Ideal para auditoria e inspeção manual dos dados brutos
        + Suporte nativo em praticamente toda ferramenta de dados

    Desvantagens:
        - Sem tipagem — tudo é string, tipos precisam ser inferidos
        - Sem compressão — maior tamanho em disco
        - Lento para leitura de colunas específicas em arquivos grandes

    Escolha para Raw: sim — facilidade de inspeção e auditoria
    dos dados brutos justifica o tamanho maior.

    Args:
        df:      DataFrame a salvar.
        destino: Caminho completo do arquivo .csv.

    Returns:
        Dict com metadados do arquivo gerado.
    """
    destino.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(destino, index=False, encoding="utf-8-sig")
    tamanho = destino.stat().st_size
    log.info("CSV salvo: %s (%s KB)", destino.name, f"{tamanho/1024:.1f}")
    return _metadados(destino, "csv", len(df), tamanho)


def salvar_json(df: pd.DataFrame, destino: Path, metadados_extras: dict = None) -> dict:
    """
    Salva o DataFrame em formato JSON (orientação records).

    Vantagens do JSON:
        + Suporta estruturas aninhadas (ex.: arrays, objetos dentro de campos)
        + Ideal para APIs REST e sistemas que consomem JSON nativamente
        + Preserva metadados junto aos dados (fonte, timestamp, versão)
        + Legível por humanos

    Desvantagens:
        - Verboso — repete os nomes de campo em cada registro
        - Maior que CSV para dados tabulares simples
        - Sem compressão nativa
        - Parsing mais lento que CSV em pandas para dados grandes

    Escolha para Raw: sim — permite incluir envelope com metadados
    (fonte, URL, timestamp) junto ao payload de dados.

    Args:
        df:               DataFrame a salvar.
        destino:          Caminho completo do arquivo .json.
        metadados_extras: Campos adicionais para o envelope JSON.

    Returns:
        Dict com metadados do arquivo gerado.
    """
    destino.parent.mkdir(parents=True, exist_ok=True)

    envelope = {
        "fonte"      : "IBGE/PAM-SIDRA",
        "tabela"     : "5457",
        "coletado_em": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_registros": len(df),
        **(metadados_extras or {}),
        "registros"  : json.loads(
            df.to_json(orient="records", force_ascii=False, date_format="iso")
        ),
    }

    with open(destino, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False, indent=2, default=str)

    tamanho = destino.stat().st_size
    log.info("JSON salvo: %s (%s KB)", destino.name, f"{tamanho/1024:.1f}")
    return _metadados(destino, "json", len(df), tamanho)


def salvar_parquet(df: pd.DataFrame, destino: Path) -> Optional[dict]:
    """
    Salva o DataFrame em formato Parquet com compressão Snappy.

    Vantagens do Parquet:
        + Armazenamento colunar — leitura de colunas específicas sem
          carregar o arquivo inteiro (fundamental para Big Data)
        + Compressão nativa (Snappy, Gzip, ZSTD) — até 10x menor que CSV
        + Preserva tipos de dado (int64, float64, datetime) sem inferência
        + Compatível com Spark, Athena, Redshift Spectrum, DuckDB, BigQuery
        + Padrão de facto em Data Lakes (AWS S3 + Athena, Azure ADLS)

    Desvantagens:
        - Não legível por humanos (binário)
        - Requer pyarrow ou fastparquet para leitura/escrita
        - Menos adequado para arquivos pequenos (overhead de metadados)

    Escolha para Raw: recomendado para volumes grandes (nível município)
    e como formato preferido em pipelines de dados modernos.

    Requer:
        pip install pyarrow

    Args:
        df:      DataFrame a salvar.
        destino: Caminho completo do arquivo .parquet.

    Returns:
        Dict com metadados do arquivo gerado, ou None se pyarrow
        não estiver instalado.
    """
    destino.parent.mkdir(parents=True, exist_ok=True)

    try:
        df.to_parquet(destino, index=False, compression="snappy")
        tamanho = destino.stat().st_size
        log.info("Parquet salvo: %s (%s KB)", destino.name, f"{tamanho/1024:.1f}")
        return _metadados(destino, "parquet", len(df), tamanho)

    except ImportError:
        log.warning(
            "pyarrow não instalado — Parquet ignorado. "
            "Execute: pip install pyarrow"
        )
        return None


# ──────────────────────────────────────────────────────────────
#  ORQUESTRADOR
# ──────────────────────────────────────────────────────────────

def salvar_raw(
    df: pd.DataFrame,
    pasta_base: Path,
    nivel: str,
    anos: list[int],
) -> dict:
    """
    Salva os dados brutos nos 3 formatos e gera o manifesto.

    Esta é a função principal da camada Raw. Ela não transforma
    os dados — apenas os persiste em múltiplos formatos para
    garantir compatibilidade com diferentes ferramentas downstream.

    Estrutura criada:
        {pasta_base}/
        ├── csv/     ibge_pam_{nivel}_{anos}_{data}.csv
        ├── json/    ibge_pam_{nivel}_{anos}_{data}.json
        ├── parquet/ ibge_pam_{nivel}_{anos}_{data}.parquet
        └── _manifesto.json

    Args:
        df:         DataFrame bruto retornado pelo scraper.
        pasta_base: Raiz da camada raw (ex.: Path("data/raw")).
        nivel:      Nível territorial coletado (brasil/uf/municipio).
        anos:       Lista de anos coletados.

    Returns:
        Dict com o manifesto completo (paths, tamanhos, timestamps).
    """
    hoje      = datetime.now().strftime("%Y%m%d")
    anos_str  = "_".join(str(a) for a in sorted(anos))
    base_nome = f"ibge_pam_{nivel}_{anos_str}_{hoje}"

    manifesto = {
        "pipeline"   : "agro-data-pipeline",
        "camada"     : "raw",
        "fonte"      : "IBGE/PAM-SIDRA",
        "tabela"     : "5457",
        "nivel"      : nivel,
        "anos"       : anos,
        "total_linhas": len(df),
        "gerado_em"  : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "arquivos"   : {},
    }

    # Salva CSV
    meta_csv = salvar_csv(df, pasta_base / "csv" / f"{base_nome}.csv")
    manifesto["arquivos"]["csv"] = meta_csv

    # Salva JSON (com envelope de metadados)
    meta_json = salvar_json(
        df,
        pasta_base / "json" / f"{base_nome}.json",
        metadados_extras={"nivel": nivel, "anos": anos},
    )
    manifesto["arquivos"]["json"] = meta_json

    # Salva Parquet (opcional, requer pyarrow)
    meta_parquet = salvar_parquet(df, pasta_base / "parquet" / f"{base_nome}.parquet")
    if meta_parquet:
        manifesto["arquivos"]["parquet"] = meta_parquet

    # Grava manifesto
    _gravar_manifesto(manifesto, pasta_base)

    # Loga comparativo de tamanhos
    _logar_comparativo(manifesto)

    return manifesto


# ──────────────────────────────────────────────────────────────
#  MANIFESTO
# ──────────────────────────────────────────────────────────────

def _metadados(arq: Path, formato: str, n_linhas: int, tamanho_bytes: int) -> dict:
    """
    Gera o dict de metadados de um arquivo salvo.

    Args:
        arq:           Path do arquivo.
        formato:       "csv", "json" ou "parquet".
        n_linhas:      Número de linhas de dados.
        tamanho_bytes: Tamanho em bytes do arquivo.

    Returns:
        Dict com path, formato, linhas, tamanho e timestamp.
    """
    return {
        "path"          : str(arq),
        "formato"       : formato,
        "linhas"        : n_linhas,
        "tamanho_bytes" : tamanho_bytes,
        "tamanho_kb"    : round(tamanho_bytes / 1024, 2),
        "salvo_em"      : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _gravar_manifesto(manifesto: dict, pasta_base: Path) -> None:
    """
    Grava o manifesto JSON na raiz da camada raw.

    O manifesto registra todos os arquivos salvos, seus tamanhos
    e metadados. Permite rastrear o que foi coletado sem abrir
    os arquivos de dados.

    Args:
        manifesto:  Dict retornado por salvar_raw().
        pasta_base: Raiz da camada raw.
    """
    caminho = pasta_base / "_manifesto.json"
    pasta_base.mkdir(parents=True, exist_ok=True)
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(manifesto, f, ensure_ascii=False, indent=2, default=str)
    log.info("Manifesto gravado: %s", caminho)


def _logar_comparativo(manifesto: dict) -> None:
    """
    Registra via logging a comparação de tamanho entre os formatos.

    Args:
        manifesto: Dict retornado por salvar_raw().
    """
    arquivos = manifesto.get("arquivos", {})
    if not arquivos:
        return

    log.info("─" * 50)
    log.info("Comparativo de tamanho — camada raw")
    log.info("─" * 50)

    baseline = arquivos.get("csv", {}).get("tamanho_bytes", 1)
    for fmt, meta in arquivos.items():
        tb  = meta["tamanho_bytes"]
        pct = tb / baseline * 100
        log.info(
            "  %-8s %8.1f KB   (%5.1f%% do CSV)",
            fmt.upper(), meta["tamanho_kb"], pct,
        )
    log.info("─" * 50)


# ──────────────────────────────────────────────────────────────
#  EXECUÇÃO DIRETA (demonstração)
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    # Dados de demonstração (subset real da API IBGE/SIDRA)
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    url   = "https://apisidra.ibge.gov.br/values/t/5457/n1/all/v/214,215,112,216/p/{ano}/c782/{cod}"

    demo = pd.DataFrame([
        {"produto":"Soja (em grão)","produto_cod":"40124","nivel_territorial":"Brasil","cod_territorial":"1","nome_territorial":"Brasil","ano":2024,"area_colhida_ha":None,"qtd_produzida":None,"unidade_qtd":"Toneladas","rendimento_medio_kg_ha":None,"valor_producao_mil_reais":260233510,"fonte":"IBGE/PAM-SIDRA","tabela_sidra":"5457","url_origem":url.format(ano=2024,cod=40124),"coletado_em":agora},
        {"produto":"Soja (em grão)","produto_cod":"40124","nivel_territorial":"Brasil","cod_territorial":"1","nome_territorial":"Brasil","ano":2023,"area_colhida_ha":45056476,"qtd_produzida":162360628,"unidade_qtd":"Toneladas","rendimento_medio_kg_ha":3604,"valor_producao_mil_reais":245876543,"fonte":"IBGE/PAM-SIDRA","tabela_sidra":"5457","url_origem":url.format(ano=2023,cod=40124),"coletado_em":agora},
        {"produto":"Milho (em grão)","produto_cod":"40125","nivel_territorial":"Brasil","cod_territorial":"1","nome_territorial":"Brasil","ano":2023,"area_colhida_ha":22740065,"qtd_produzida":137001311,"unidade_qtd":"Toneladas","rendimento_medio_kg_ha":6026,"valor_producao_mil_reais":77434827,"fonte":"IBGE/PAM-SIDRA","tabela_sidra":"5457","url_origem":url.format(ano=2023,cod=40125),"coletado_em":agora},
        {"produto":"Café (em grão) Total","produto_cod":"40132","nivel_territorial":"Brasil","cod_territorial":"1","nome_territorial":"Brasil","ano":2023,"area_colhida_ha":2194684,"qtd_produzida":3557390,"unidade_qtd":"Toneladas","rendimento_medio_kg_ha":1621,"valor_producao_mil_reais":49540313,"fonte":"IBGE/PAM-SIDRA","tabela_sidra":"5457","url_origem":url.format(ano=2023,cod=40132),"coletado_em":agora},
        {"produto":"Cana-de-açúcar","produto_cod":"40135","nivel_territorial":"Brasil","cod_territorial":"1","nome_territorial":"Brasil","ano":2023,"area_colhida_ha":8614088,"qtd_produzida":672270543,"unidade_qtd":"Toneladas","rendimento_medio_kg_ha":78043,"valor_producao_mil_reais":79131817,"fonte":"IBGE/PAM-SIDRA","tabela_sidra":"5457","url_origem":url.format(ano=2023,cod=40135),"coletado_em":agora},
    ])

    manifesto = salvar_raw(demo, Path("data/raw"), "brasil", [2023, 2024])
    print("\nManifesto gerado:")
    print(json.dumps(manifesto, indent=2, default=str))