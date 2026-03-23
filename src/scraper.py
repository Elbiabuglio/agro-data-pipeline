"""
ibge_sidra_pam.py
=================
Scraper da API IBGE/SIDRA — Produção Agrícola Municipal (PAM).

Coleta dados da Tabela 5457 do SIDRA para as principais commodities
agrícolas brasileiras (soja, milho, café, cana, algodão, etc.),
referentes aos anos configurados em ANOS, e salva o resultado
em CSV na pasta data/raw/.

Fonte oficial:
    https://apisidra.ibge.gov.br
    https://sidra.ibge.gov.br/tabela/5457

Variáveis coletadas (Tabela 5457):
    v214 → Área colhida (Hectares)
    v215 → Quantidade produzida (unidade varia por produto)
    v112 → Rendimento médio da produção (Kg/ha)
    v216 → Valor da produção (Mil Reais)

Execução:
    python src/teste.py
"""

import os
import sys
import time
import random
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

import requests
import pandas as pd
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env (se existir)
load_dotenv()


# ──────────────────────────────────────────────────────────────
#  CONFIGURAÇÃO — lida do .env com fallback para valores padrão
# ──────────────────────────────────────────────────────────────

# ANOS=2023,2024  →  [2023, 2024]
ANOS = [
    int(a.strip())
    for a in os.getenv("ANOS", "2023,2024").split(",")
]

# NIVEL=brasil  →  "brasil"
NIVEL = os.getenv("NIVEL", "brasil")

# OUTPUT_DIR=data/raw  →  Path("data/raw")
PASTA_SAIDA = Path(os.getenv("OUTPUT_DIR", "data/raw"))


# ──────────────────────────────────────────────────────────────
#  CONSTANTES — API IBGE/SIDRA
#  Documentação: https://apisidra.ibge.gov.br/home/ajuda
# ──────────────────────────────────────────────────────────────

API_BASE = os.getenv("API_BASE_URL", "https://apisidra.ibge.gov.br/values")
TABELA   = os.getenv("TABELA_SIDRA", "5457")

NIVEIS: dict[str, str] = {
    "brasil"   : "n1/all",   # 1 registro por produto/ano
    "uf"       : "n3/all",   # 27 UFs por produto/ano
    "municipio": "n6/all",   # 5 565 municípios por produto/ano
}

# Mapeamento: código da variável → nome da coluna no CSV final
VARIAVEIS: dict[str, str] = {
    "214": "area_colhida_ha",
    "215": "qtd_produzida",           # unidade varia por produto
    "112": "rendimento_medio_kg_ha",
    "216": "valor_producao_mil_reais",
}

# Classificação 782 — Produto das lavouras
# Códigos confirmados no descritor oficial da Tabela 5457
COMMODITIES: dict[str, tuple[str, str]] = {
    # chave          : (código_sidra, nome_oficial_ibge)
    "soja"           : ("40124", "Soja (em grão)"),
    "milho"          : ("40125", "Milho (em grão)"),
    "algodao"        : ("40126", "Algodão herbáceo (em caroço)"),
    "amendoim"       : ("40127", "Amendoim (em casca)"),
    "arroz"          : ("40128", "Arroz (em casca)"),
    "feijao"         : ("40131", "Feijão (em grão)"),
    "cafe"           : ("40132", "Café (em grão) Total"),
    "trigo"          : ("40133", "Trigo (em grão)"),
    "cana"           : ("40135", "Cana-de-açúcar"),
    "mandioca"       : ("40138", "Mandioca"),
    "sorgo"          : ("40141", "Sorgo (em grão)"),
    "girassol"       : ("40147", "Girassol (em grão)"),
    "aveia"          : ("40148", "Aveia (em grão)"),
    "cevada"         : ("40149", "Cevada (em grão)"),
    "triticale"      : ("40150", "Triticale (em grão)"),
    "laranja"        : ("40199", "Laranja"),
    "banana"         : ("40186", "Banana (cacho)"),
    "cacau"          : ("40190", "Cacau (em amêndoa)"),
    "borracha"       : ("40218", "Borracha (látex coagulado)"),
    "sisal"          : ("40228", "Sisal ou agave (fibra)"),
}

# Ordem das colunas no CSV de saída
COLUNAS_CSV: list[str] = [
    "produto", "produto_cod",
    "nivel_territorial", "cod_territorial", "nome_territorial",
    "ano",
    "area_colhida_ha", "qtd_produzida", "unidade_qtd",
    "rendimento_medio_kg_ha", "valor_producao_mil_reais",
    "fonte", "tabela_sidra", "url_origem", "coletado_em",
]

BLOCO = 8  # número máximo de produtos por requisição (evita URLs longas)


# ──────────────────────────────────────────────────────────────
#  LOGGING
#  Saída simultânea: console (INFO) + arquivo (DEBUG completo)
# ──────────────────────────────────────────────────────────────

def configurar_logging() -> logging.Logger:
    """
    Configura e retorna o logger da aplicação.

    Handlers:
        - StreamHandler → stdout, nível INFO
        - FileHandler   → sidra_scraper.log, nível DEBUG

    Returns:
        Logger configurado com o nome "sidra".
    """
    fmt = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("sidra")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    # Console — INFO
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(fmt, datefmt))
    logger.addHandler(ch)

    # Arquivo — DEBUG (inclui URLs completas e contagens detalhadas)
    fh = logging.FileHandler("sidra_scraper.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(fmt, datefmt))
    logger.addHandler(fh)

    return logger


log = configurar_logging()


# ──────────────────────────────────────────────────────────────
#  SESSÃO HTTP
# ──────────────────────────────────────────────────────────────

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; IBGE-SIDRA-Research/1.0)",
    "Accept"    : "application/json",
})


# ──────────────────────────────────────────────────────────────
#  CAMADA HTTP
# ──────────────────────────────────────────────────────────────

def get_json(url: str, tentativas: int = 5) -> Optional[list]:
    """
    Executa um GET na URL informada e retorna a resposta como lista.

    Implementa retry com back-off linear para os seguintes casos:
        - Timeout de conexão ou leitura
        - HTTP 429 (rate limit) → aguarda 25s × tentativa
        - Erro de conexão (DNS, TCP reset)

    Args:
        url:        URL completa da API SIDRA.
        tentativas: Número máximo de tentativas (padrão: 5).

    Returns:
        Lista de dicts com a resposta JSON, ou None em caso de falha.
    """
    log.debug("GET %s", url)

    for i in range(1, tentativas + 1):
        try:
            response = SESSION.get(url, timeout=45)

            if response.status_code == 200:
                log.debug("Resposta OK — %d bytes", len(response.content))
                return response.json()

            if response.status_code == 429:
                wait = 25 * i
                log.warning("Rate limit (429). Aguardando %ds antes de tentar novamente...", wait)
                time.sleep(wait)

            else:
                log.error("HTTP %d inesperado para URL: %s", response.status_code, url)
                return None

        except requests.exceptions.Timeout:
            log.warning("Timeout na tentativa %d/%d. Aguardando %ds...", i, tentativas, 8 * i)
            time.sleep(8 * i)

        except requests.exceptions.ConnectionError as exc:
            log.error("Erro de conexão: %s", exc)
            time.sleep(10)

        except ValueError:
            log.error("Resposta não é JSON válido. URL: %s", url)
            return None

    log.error("Todas as %d tentativas falharam para: %s", tentativas, url)
    return None


# ──────────────────────────────────────────────────────────────
#  PARSING
# ──────────────────────────────────────────────────────────────

def safe_num(valor) -> Optional[float]:
    """
    Converte um valor retornado pela API SIDRA para float.

    A API SIDRA usa convenções especiais para dados ausentes:
        "-"   → dado não disponível para o período
        "..." → dado ainda em apuração
        "X"   → dado sigiloso (municípios com poucos produtores)
        ""    → campo vazio

    Todos os casos acima retornam None (NaN no DataFrame).
    Números no formato brasileiro (1.234,56) são normalizados para float.

    Args:
        valor: Valor bruto retornado pelo campo "V" da API.

    Returns:
        float se conversível, None caso contrário.
    """
    texto = str(valor).strip()
    if texto in ("-", "...", "X", ""):
        return None
    try:
        return float(texto.replace(".", "").replace(",", "."))
    except ValueError:
        log.debug("Valor não conversível para float: %r", texto)
        return None


def parse_sidra(rows: list[dict], url_origem: str) -> list[dict]:
    """
    Transforma a resposta JSON da API SIDRA em lista de dicts normalizados.

    Estrutura real da API SIDRA (Tabela 5457):
        rows[0]  → cabeçalho (metadados das chaves, não contém dados)
        rows[1:] → dados, um dict por célula no formato:
            "D1C"/"D1N" → código/nome do território
            "D2C"/"D2N" → código/nome da variável (ex.: Área colhida)
            "D3C"/"D3N" → código/ano de referência
            "D4C"/"D4N" → código/nome do produto das lavouras
            "MC"/"MN"   → código/nome da unidade de medida
            "NC"/"NN"   → código/nome do nível territorial
            "V"         → valor (string; pode ser "-", "...", "X")

    Cada linha representa uma combinação única de:
        (território × variável × ano × produto)

    O pivô para formato largo (uma linha por registro) é feito
    posteriormente em pivotar().

    Args:
        rows:       Lista de dicts retornada pela API SIDRA.
        url_origem: URL da requisição, gravada para rastreabilidade.

    Returns:
        Lista de dicts normalizados, um por célula de dado.
    """
    if not rows or len(rows) < 2:
        log.warning("Resposta vazia ou sem dados (apenas cabeçalho).")
        return []

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
            "coletado_em"      : timestamp,
        })

    log.debug("parse_sidra: %d registros extraídos de %d linhas brutas.",
              len(registros), len(rows) - 1)
    return registros


def pivotar(registros: list[dict]) -> pd.DataFrame:
    """
    Converte os registros de formato longo para formato largo.

    A API SIDRA retorna uma linha por variável para cada
    combinação (produto × ano × território). Esta função pivota
    esse formato para que cada combinação resulte em uma única
    linha com as variáveis como colunas.

    Exemplo de transformação:
        Entrada (longo):
            produto | ano  | variavel_col        | valor
            Soja    | 2023 | area_colhida_ha     | 45_056_476
            Soja    | 2023 | qtd_produzida       | 162_360_628
            Soja    | 2023 | valor_producao_...  | 245_876_543

        Saída (largo):
            produto | ano  | area_colhida_ha | qtd_produzida | valor_producao_...
            Soja    | 2023 | 45_056_476      | 162_360_628   | 245_876_543

    A coluna "unidade_qtd" é preservada separadamente antes do
    pivot, pois varia por produto (ex.: "Toneladas" para grãos,
    "Mil frutos" para laranja, "Mil cachos" para banana).

    Args:
        registros: Lista de dicts no formato longo retornada por parse_sidra().

    Returns:
        DataFrame no formato largo, ou DataFrame vazio se a entrada
        for vazia ou o pivot falhar.
    """
    if not registros:
        log.warning("pivotar: nenhum registro para pivotar.")
        return pd.DataFrame()

    df_longo = pd.DataFrame(registros)
    log.debug("pivotar: %d linhas no formato longo.", len(df_longo))

    # Salva a unidade de qtd_produzida antes do pivot
    unid_qtd = (
        df_longo[df_longo["variavel_col"] == "qtd_produzida"]
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
        df_largo = df_longo.pivot_table(
            index=id_cols,
            columns="variavel_col",
            values="valor",
            aggfunc="first",
        ).reset_index()
        df_largo.columns.name = None
    except Exception as exc:
        log.error("Pivot falhou: %s. Retornando formato longo como fallback.", exc)
        return df_longo

    df_largo = df_largo.merge(
        unid_qtd, on=["produto_cod", "ano", "cod_territorial"], how="left"
    )

    # Garante que todas as colunas de variável existam,
    # mesmo que a API não tenha retornado alguma delas
    for col in VARIAVEIS.values():
        if col not in df_largo.columns:
            df_largo[col] = None

    log.debug("pivotar: %d linhas no formato largo.", len(df_largo))
    return df_largo


# ──────────────────────────────────────────────────────────────
#  COLETA
# ──────────────────────────────────────────────────────────────

def coletar() -> pd.DataFrame:
    """
    Orquestra a coleta de dados de todas as commodities via API SIDRA.

    Divide os produtos em blocos de BLOCO itens para evitar URLs
    excessivamente longas (limite prático ~2 000 caracteres).
    Aplica pausa aleatória entre blocos para respeitar a API pública.

    Fluxo por bloco:
        1. Monta URL com nível territorial, variáveis, anos e produtos
        2. Faz GET com retry via get_json()
        3. Parseia a resposta com parse_sidra()
        4. Acumula registros

    Após todos os blocos:
        5. Pivota para formato largo com pivotar()
        6. Corrige tipos de dado (ano → Int64, variáveis → float)
        7. Ordena por produto / ano / território

    Returns:
        DataFrame consolidado com todos os produtos e anos,
        ou DataFrame vazio se nenhum bloco retornar dados.
    """
    nivel_str = NIVEIS[NIVEL]
    vars_list = list(VARIAVEIS.keys())
    chaves    = list(COMMODITIES.keys())
    todos: list[dict] = []

    blocos = [chaves[i:i + BLOCO] for i in range(0, len(chaves), BLOCO)]

    log.info(
        "Iniciando coleta — %d produtos | %d blocos | anos: %s | nível: %s",
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
            log.warning("Bloco %d não retornou dados. Prosseguindo.", n)
            time.sleep(3)
            continue

        regs = parse_sidra(rows, url)
        log.info("Bloco %d/%d: %d registros recebidos.", n, len(blocos), len(regs))
        todos.extend(regs)

        if n < len(blocos):
            pausa = random.uniform(1.5, 3.0)
            log.debug("Pausa de %.1fs entre blocos.", pausa)
            time.sleep(pausa)

    if not todos:
        log.error("Coleta finalizada sem nenhum dado. Verifique a conexão.")
        return pd.DataFrame()

    log.info("Total de registros brutos: %d. Iniciando pivot...", len(todos))
    df = pivotar(todos)

    # Tipagem
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
    for col in VARIAVEIS.values():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values(["produto", "ano", "nome_territorial"]).reset_index(drop=True)
    log.info("DataFrame final: %d linhas × %d colunas.", len(df), len(df.columns))
    return df


# ──────────────────────────────────────────────────────────────
#  SALVAMENTO
# ──────────────────────────────────────────────────────────────

def salvar(df: pd.DataFrame) -> Path:
    """
    Salva o DataFrame em CSV na pasta configurada em PASTA_SAIDA.

    Nome do arquivo gerado:
        ibge_pam_{nivel}_{anos}_{YYYYMMDD}.csv
        Ex.: ibge_pam_brasil_2023_2024_20240322.csv

    Encoding: UTF-8 com BOM (utf-8-sig) para compatibilidade com Excel.
    Colunas: ordenadas conforme COLUNAS_CSV; colunas extras são ignoradas.

    Args:
        df: DataFrame retornado por coletar().

    Returns:
        Path do arquivo CSV criado.
    """
    PASTA_SAIDA.mkdir(parents=True, exist_ok=True)

    hoje  = datetime.now().strftime("%Y%m%d")
    anos  = "_".join(str(a) for a in ANOS)
    nome  = f"ibge_pam_{NIVEL}_{anos}_{hoje}.csv"
    arq   = PASTA_SAIDA / nome

    cols = [c for c in COLUNAS_CSV if c in df.columns]
    df[cols].to_csv(arq, index=False, encoding="utf-8-sig")

    log.info("CSV salvo em: %s (%d linhas, %d colunas)", arq, len(df), len(cols))
    return arq


# ──────────────────────────────────────────────────────────────
#  RELATÓRIO FINAL
# ──────────────────────────────────────────────────────────────

def sumario(df: pd.DataFrame, arq: Path) -> None:
    """
    Registra via logging um resumo tabular da coleta realizada.

    Exibe por produto e ano:
        - Quantidade produzida (toneladas ou unidade do produto)
        - Valor da produção (Mil Reais)

    Args:
        df:  DataFrame retornado por coletar().
        arq: Caminho do CSV salvo, exibido ao final do relatório.
    """
    separador = "=" * 70
    linha_vazia = ""

    log.info(linha_vazia)
    log.info(separador)
    log.info("RESULTADO — IBGE/SIDRA PAM Tabela %s", TABELA)
    log.info(separador)
    log.info(
        "  %-35s %5s  %18s  %16s",
        "Produto", "Ano", "Qtd Produzida", "Valor (Mil R$)",
    )
    log.info("  " + "-" * 66)

    qtd_col = "qtd_produzida"
    val_col = "valor_producao_mil_reais"

    for _, row in df.iterrows():
        prod  = str(row.get("produto", ""))[:34]
        ano   = str(row.get("ano", ""))
        qtd   = row.get(qtd_col)
        val   = row.get(val_col)
        qtd_s = f"{qtd:>18,.0f}" if pd.notna(qtd) else f"{'—':>18}"
        val_s = f"{val:>16,.0f}" if pd.notna(val) else f"{'—':>16}"
        log.info("  %-35s %5s  %s  %s", prod, ano, qtd_s, val_s)

    log.info(separador)
    log.info("Total de linhas : %d", len(df))
    log.info("Arquivo salvo   : %s", arq)
    log.info(separador)


# ──────────────────────────────────────────────────────────────
#  ENTRY POINT
# ──────────────────────────────────────────────────────────────

def main() -> None:
    """
    Ponto de entrada da aplicação.

    Sequência de execução:
        1. Log de início com parâmetros de configuração
        2. coletar()  → busca dados na API SIDRA
        3. salvar()   → persiste CSV em data/raw/
        4. sumario()  → relatório final via logging
    """
    log.info("=" * 55)
    log.info("IBGE/SIDRA — PAM Tabela %s", TABELA)
    log.info("Anos    : %s", ANOS)
    log.info("Nível   : %s (%s)", NIVEL, NIVEIS[NIVEL])
    log.info("Saída   : %s", PASTA_SAIDA.resolve())
    log.info("=" * 55)

    df = coletar()

    if df.empty:
        log.error("Nenhum dado coletado. Encerrando.")
        sys.exit(1)

    arq = salvar(df)
    sumario(df, arq)
    log.info("Execução concluída com sucesso.")


if __name__ == "__main__":
    main()