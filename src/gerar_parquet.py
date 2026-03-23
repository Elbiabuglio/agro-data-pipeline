"""
gerar_parquet.py
================
Coleta dados da API IBGE/SIDRA e salva os 3 formatos da camada Raw:
    CSV, JSON e Parquet (compressão Snappy)

Todos os arquivos seguem o mesmo padrão de nome:
    ibge_pam_{nivel}_{anos}_{YYYYMMDD}.{ext}

Estrutura gerada:
    data/raw/
    ├── csv/     ibge_pam_brasil_2023_2024_YYYYMMDD.csv
    ├── json/    ibge_pam_brasil_2023_2024_YYYYMMDD.json
    ├── parquet/ ibge_pam_brasil_2023_2024_YYYYMMDD.parquet
    └── _manifesto.json

Execução:
    python src/gerar_parquet.py
"""

import json
import logging
import os
import random
import sys
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()


# ──────────────────────────────────────────────────────────────
#  CONFIGURAÇÃO
# ──────────────────────────────────────────────────────────────

ANOS        = [int(a.strip()) for a in os.getenv("ANOS", "2023,2024").split(",")]
NIVEL       = os.getenv("NIVEL", "brasil")
PASTA_SAIDA = Path(os.getenv("OUTPUT_DIR", "data/raw"))
API_BASE    = os.getenv("API_BASE_URL", "https://apisidra.ibge.gov.br/values")
TABELA      = os.getenv("TABELA_SIDRA", "5457")


# ──────────────────────────────────────────────────────────────
#  CONSTANTES API
# ──────────────────────────────────────────────────────────────

NIVEIS: dict[str, str] = {
    "brasil"   : "n1/all",
    "uf"       : "n3/all",
    "municipio": "n6/all",
}

VARIAVEIS: dict[str, str] = {
    "214": "area_colhida_ha",
    "215": "qtd_produzida",
    "112": "rendimento_medio_kg_ha",
    "216": "valor_producao_mil_reais",
}

COMMODITIES: dict[str, tuple[str, str]] = {
    "soja"      : ("40124", "Soja (em grão)"),
    "milho"     : ("40125", "Milho (em grão)"),
    "algodao"   : ("40126", "Algodão herbáceo (em caroço)"),
    "amendoim"  : ("40127", "Amendoim (em casca)"),
    "arroz"     : ("40128", "Arroz (em casca)"),
    "feijao"    : ("40131", "Feijão (em grão)"),
    "cafe"      : ("40132", "Café (em grão) Total"),
    "trigo"     : ("40133", "Trigo (em grão)"),
    "cana"      : ("40135", "Cana-de-açúcar"),
    "mandioca"  : ("40138", "Mandioca"),
    "sorgo"     : ("40141", "Sorgo (em grão)"),
    "girassol"  : ("40147", "Girassol (em grão)"),
    "aveia"     : ("40148", "Aveia (em grão)"),
    "cevada"    : ("40149", "Cevada (em grão)"),
    "triticale" : ("40150", "Triticale (em grão)"),
    "laranja"   : ("40199", "Laranja"),
    "banana"    : ("40186", "Banana (cacho)"),
    "cacau"     : ("40190", "Cacau (em amêndoa)"),
    "borracha"  : ("40218", "Borracha (látex coagulado)"),
    "sisal"     : ("40228", "Sisal ou agave (fibra)"),
}

COLUNAS_CSV: list[str] = [
    "produto", "produto_cod",
    "nivel_territorial", "cod_territorial", "nome_territorial",
    "ano",
    "area_colhida_ha", "qtd_produzida", "unidade_qtd",
    "rendimento_medio_kg_ha", "valor_producao_mil_reais",
    "fonte", "tabela_sidra", "url_origem", "coletado_em",
]

BLOCO = 8


# ──────────────────────────────────────────────────────────────
#  LOGGING
# ──────────────────────────────────────────────────────────────

def configurar_logging() -> logging.Logger:
    """
    Configura o logger com dois handlers:
        - Console (INFO)
        - Arquivo sidra_scraper.log (DEBUG)

    Returns:
        Logger configurado com o nome "sidra.raw".
    """
    fmt     = "%(asctime)s [%(levelname)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("sidra.raw")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(fmt, datefmt))
    logger.addHandler(ch)

    fh = logging.FileHandler("sidra_scraper.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(fmt, datefmt))
    logger.addHandler(fh)

    return logger


log = configurar_logging()

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; IBGE-SIDRA-Research/1.0)",
    "Accept"    : "application/json",
})


# ──────────────────────────────────────────────────────────────
#  HTTP
# ──────────────────────────────────────────────────────────────

def get_json(url: str, tentativas: int = 5) -> Optional[list]:
    """
    GET com retry exponencial.

    Args:
        url:        URL da API SIDRA.
        tentativas: Máximo de tentativas (padrão: 5).

    Returns:
        Lista de dicts com a resposta JSON, ou None em caso de falha.
    """
    log.debug("GET %s", url)
    for i in range(1, tentativas + 1):
        try:
            r = SESSION.get(url, timeout=45)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                wait = 25 * i
                log.warning("Rate limit. Aguardando %ds...", wait)
                time.sleep(wait)
            else:
                log.error("HTTP %d: %s", r.status_code, url)
                return None
        except requests.exceptions.Timeout:
            log.warning("Timeout. Tentativa %d/%d", i, tentativas)
            time.sleep(8 * i)
        except requests.exceptions.ConnectionError as exc:
            log.error("Conexão: %s", exc)
            time.sleep(10)
        except ValueError:
            log.error("JSON inválido: %s", url)
            return None
    return None


# ──────────────────────────────────────────────────────────────
#  PARSING
# ──────────────────────────────────────────────────────────────

def safe_num(valor) -> Optional[float]:
    """
    Converte valor da API SIDRA para float.
    Retorna None para: '-', '...', 'X', '' (ausência de dado).

    Args:
        valor: Valor bruto do campo 'V' da API.

    Returns:
        float se conversível, None caso contrário.
    """
    s = str(valor).strip()
    if s in ("-", "...", "X", ""):
        return None
    try:
        return float(s.replace(".", "").replace(",", "."))
    except ValueError:
        return None


def parse_sidra(rows: list[dict], url_origem: str) -> list[dict]:
    """
    Transforma a resposta JSON da API SIDRA em lista de dicts normalizados.
    rows[0] é o cabeçalho e é ignorado; rows[1:] contém os dados.

    Args:
        rows:       Lista retornada pela API SIDRA.
        url_origem: URL da requisição (para rastreabilidade).

    Returns:
        Lista de dicts com os dados normalizados no formato longo.
    """
    if not rows or len(rows) < 2:
        return []

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    registros = []
    for linha in rows[1:]:
        var_cod  = linha.get("D2C", "")
        col_nome = VARIAVEIS.get(var_cod, f"var_{var_cod}")
        registros.append({
            "produto"          : linha.get("D4N", ""),
            "produto_cod"      : linha.get("D4C", ""),
            "nivel_territorial": linha.get("NN",  ""),
            "cod_territorial"  : linha.get("D1C", ""),
            "nome_territorial" : linha.get("D1N", ""),
            "ano"              : linha.get("D3N", ""),
            "variavel_cod"     : var_cod,
            "variavel_col"     : col_nome,
            "unidade_api"      : linha.get("MN",  ""),
            "valor"            : safe_num(linha.get("V", "")),
            "fonte"            : "IBGE/PAM-SIDRA",
            "tabela_sidra"     : TABELA,
            "url_origem"       : url_origem,
            "coletado_em"      : ts,
        })
    return registros


def pivotar(registros: list[dict]) -> pd.DataFrame:
    """
    Converte formato longo → largo (1 linha por produto × ano × território).

    Preserva a coluna 'unidade_qtd' separadamente antes do pivot,
    pois a unidade de qtd_produzida varia por produto.

    Args:
        registros: Lista de dicts no formato longo.

    Returns:
        DataFrame no formato largo, ou DataFrame vazio se entrada vazia.
    """
    if not registros:
        return pd.DataFrame()

    df = pd.DataFrame(registros)

    unid_qtd = (
        df[df["variavel_col"] == "qtd_produzida"]
        [["produto_cod", "ano", "cod_territorial", "unidade_api"]]
        .drop_duplicates()
        .rename(columns={"unidade_api": "unidade_qtd"})
    )

    id_cols = [
        "produto", "produto_cod", "nivel_territorial",
        "cod_territorial", "nome_territorial", "ano",
        "fonte", "tabela_sidra", "url_origem", "coletado_em",
    ]

    try:
        df_largo = df.pivot_table(
            index=id_cols,
            columns="variavel_col",
            values="valor",
            aggfunc="first",
        ).reset_index()
        df_largo.columns.name = None
    except Exception as exc:
        log.error("Pivot falhou: %s", exc)
        return df

    df_largo = df_largo.merge(
        unid_qtd, on=["produto_cod", "ano", "cod_territorial"], how="left"
    )

    for col in VARIAVEIS.values():
        if col not in df_largo.columns:
            df_largo[col] = None

    return df_largo


# ──────────────────────────────────────────────────────────────
#  COLETA
# ──────────────────────────────────────────────────────────────

def coletar() -> pd.DataFrame:
    """
    Coleta dados de todas as commodities via API SIDRA.

    Divide os produtos em blocos de BLOCO itens por requisição
    e aplica pausa aleatória entre blocos.

    Returns:
        DataFrame consolidado no formato largo, ou DataFrame vazio.
    """
    nivel_str = NIVEIS[NIVEL]
    vars_list = list(VARIAVEIS.keys())
    chaves    = list(COMMODITIES.keys())
    todos: list[dict] = []

    blocos = [chaves[i:i + BLOCO] for i in range(0, len(chaves), BLOCO)]

    log.info(
        "Coletando %d produtos | %d blocos | anos: %s | nível: %s",
        len(chaves), len(blocos), ANOS, NIVEL,
    )

    for n, bloco in enumerate(blocos, 1):
        codigos = [COMMODITIES[k][0] for k in bloco]
        url = (
            f"{API_BASE}/t/{TABELA}/{nivel_str}"
            f"/v/{','.join(vars_list)}"
            f"/p/{','.join(str(a) for a in ANOS)}"
            f"/c782/{','.join(codigos)}"
        )

        log.info("Bloco %d/%d: %s", n, len(blocos), ", ".join(bloco))

        rows = get_json(url)
        if rows is None:
            log.warning("Bloco %d sem resposta. Prosseguindo.", n)
            time.sleep(3)
            continue

        regs = parse_sidra(rows, url)
        log.info("Bloco %d/%d: %d registros recebidos.", n, len(blocos), len(regs))
        todos.extend(regs)

        if n < len(blocos):
            time.sleep(random.uniform(1.5, 3.0))

    if not todos:
        return pd.DataFrame()

    df = pivotar(todos)
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
    for col in VARIAVEIS.values():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.sort_values(["produto", "ano", "nome_territorial"]).reset_index(drop=True)


# ──────────────────────────────────────────────────────────────
#  SALVAMENTO — 3 FORMATOS
# ──────────────────────────────────────────────────────────────

def _nome_base() -> str:
    """
    Gera o nome base dos arquivos sem extensão.

    Padrão: ibge_pam_{nivel}_{anos}_{YYYYMMDD}
    Ex.:    ibge_pam_brasil_2023_2024_20260322

    Returns:
        String com o nome base.
    """
    hoje = date.today().strftime("%Y%m%d")
    anos = "_".join(str(a) for a in sorted(ANOS))
    return f"ibge_pam_{NIVEL}_{anos}_{hoje}"


def salvar_csv(df: pd.DataFrame, nome_base: str) -> Path:
    """
    Salva o DataFrame em CSV (UTF-8 com BOM para compatibilidade com Excel).

    Args:
        df:        DataFrame a salvar.
        nome_base: Nome base sem extensão.

    Returns:
        Path do arquivo CSV gerado.
    """
    pasta = PASTA_SAIDA / "csv"
    pasta.mkdir(parents=True, exist_ok=True)
    arq = pasta / f"{nome_base}.csv"

    cols = [c for c in COLUNAS_CSV if c in df.columns]
    df[cols].to_csv(arq, index=False, encoding="utf-8-sig")

    log.info("CSV     → %s  (%.1f KB)", arq, arq.stat().st_size / 1024)
    return arq


def salvar_json(df: pd.DataFrame, nome_base: str) -> Path:
    """
    Salva o DataFrame em JSON com envelope de metadados.

    Args:
        df:        DataFrame a salvar.
        nome_base: Nome base sem extensão.

    Returns:
        Path do arquivo JSON gerado.
    """
    pasta = PASTA_SAIDA / "json"
    pasta.mkdir(parents=True, exist_ok=True)
    arq = pasta / f"{nome_base}.json"

    envelope = {
        "fonte"          : "IBGE/PAM-SIDRA",
        "tabela"         : TABELA,
        "nivel"          : NIVEL,
        "anos"           : ANOS,
        "total_registros": len(df),
        "coletado_em"    : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "registros"      : json.loads(
            df.to_json(orient="records", force_ascii=False, date_format="iso")
        ),
    }

    with open(arq, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False, indent=2, default=str)

    log.info("JSON    → %s  (%.1f KB)", arq, arq.stat().st_size / 1024)
    return arq


def salvar_parquet(df: pd.DataFrame, nome_base: str) -> Optional[Path]:
    """
    Salva o DataFrame em Parquet com compressão Snappy.

    Requer pyarrow (já presente no ambiente):
        pyarrow==21.0.0

    Args:
        df:        DataFrame a salvar.
        nome_base: Nome base sem extensão.

    Returns:
        Path do arquivo Parquet gerado, ou None se pyarrow indisponível.
    """
    pasta = PASTA_SAIDA / "parquet"
    pasta.mkdir(parents=True, exist_ok=True)
    arq = pasta / f"{nome_base}.parquet"

    try:
        df.to_parquet(arq, index=False, compression="snappy")
        log.info("Parquet → %s  (%.1f KB)", arq, arq.stat().st_size / 1024)
        return arq
    except ImportError:
        log.warning("pyarrow não encontrado — Parquet ignorado.")
        log.warning("Execute: pip install pyarrow")
        return None


def salvar_manifesto(arquivos: dict) -> None:
    """
    Grava _manifesto.json na raiz de data/raw/ com metadados
    de todos os arquivos salvos nesta execução.

    Args:
        arquivos: Dict {formato: Path} dos arquivos gerados.
    """
    PASTA_SAIDA.mkdir(parents=True, exist_ok=True)
    manifesto = {
        "pipeline"  : "agro-data-pipeline",
        "camada"    : "raw",
        "fonte"     : "IBGE/PAM-SIDRA",
        "tabela"    : TABELA,
        "nivel"     : NIVEL,
        "anos"      : ANOS,
        "gerado_em" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "arquivos"  : {
            fmt: {
                "path"         : str(arq),
                "tamanho_kb"   : round(arq.stat().st_size / 1024, 2),
                "salvo_em"     : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            for fmt, arq in arquivos.items() if arq
        },
    }
    dest = PASTA_SAIDA / "_manifesto.json"
    with open(dest, "w", encoding="utf-8") as f:
        json.dump(manifesto, f, ensure_ascii=False, indent=2)
    log.info("Manifesto → %s", dest)


# ──────────────────────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────────────────────

def main() -> None:
    """
    Coleta dados da API IBGE/SIDRA e salva nos 3 formatos da camada Raw.

    Sequência:
        1. coletar()        → busca dados na API SIDRA
        2. salvar_csv()     → data/raw/csv/
        3. salvar_json()    → data/raw/json/
        4. salvar_parquet() → data/raw/parquet/
        5. salvar_manifesto() → data/raw/_manifesto.json
    """
    log.info("=" * 55)
    log.info("IBGE/SIDRA — Camada Raw (CSV + JSON + Parquet)")
    log.info("Anos  : %s", ANOS)
    log.info("Nível : %s (%s)", NIVEL, NIVEIS[NIVEL])
    log.info("Saída : %s", PASTA_SAIDA.resolve())
    log.info("=" * 55)

    df = coletar()

    if df.empty:
        log.error("Nenhum dado coletado. Verifique a conexão.")
        sys.exit(1)

    nome = _nome_base()
    arq_csv     = salvar_csv(df, nome)
    arq_json    = salvar_json(df, nome)
    arq_parquet = salvar_parquet(df, nome)

    salvar_manifesto({
        "csv"    : arq_csv,
        "json"   : arq_json,
        "parquet": arq_parquet,
    })

    log.info("=" * 55)
    log.info("Concluído — 3 formatos salvos em %s", PASTA_SAIDA)
    log.info("=" * 55)


if __name__ == "__main__":
    main()