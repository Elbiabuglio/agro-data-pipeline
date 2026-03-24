"""
eda.py
======
Análise Exploratória de Dados (EDA) — agro-data-pipeline
Fonte: data/raw/csv/

Análises realizadas:
    1. Estatísticas descritivas (média, mediana, desvio padrão)
    2. Detecção de outliers (método IQR)
    3. Gráficos:
        - Boxplot: quantidade produzida por produto
        - Histograma: rendimento médio por produto
        - Scatter: área colhida vs valor de produção
        - Barras: valor de produção por produto (2023)

Saída:
    data/processed/graficos/
        ├── 01_boxplot_quantidade_produzida.png
        ├── 02_histograma_rendimento.png
        ├── 03_scatter_area_vs_valor.png
        └── 04_barras_valor_producao_2023.png

Execução:
    python src/eda.py
"""

import logging
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd


# ──────────────────────────────────────────────────────────────
#  CONFIGURAÇÃO
# ──────────────────────────────────────────────────────────────

PASTA_CSV      = Path("data/raw/csv")
PASTA_GRAFICOS = Path("data/processed/graficos")

plt.rcParams.update({
    "figure.dpi"      : 120,
    "figure.facecolor": "white",
    "axes.facecolor"  : "#f8f8f8",
    "axes.grid"       : True,
    "grid.alpha"      : 0.4,
    "font.size"       : 11,
    "axes.titlesize"  : 13,
    "axes.titleweight": "bold",
    "axes.labelsize"  : 11,
})

CORES = ["#2196F3", "#4CAF50", "#FF9800", "#E91E63",
         "#9C27B0", "#00BCD4", "#FF5722", "#607D8B"]

MAPA_PRODUTO = {
    "Soja (em grão)"              : "Soja",
    "Milho (em grão)"             : "Milho",
    "Café (em grão) Total"        : "Café",
    "Cana-de-açúcar"              : "Cana",
    "Algodão herbáceo (em caroço)": "Algodão",
    "Trigo (em grão)"             : "Trigo",
    "Arroz (em casca)"            : "Arroz",
    "Feijão (em grão)"            : "Feijão",
}


# ──────────────────────────────────────────────────────────────
#  LOGGING
# ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("eda")


# ──────────────────────────────────────────────────────────────
#  UTILITÁRIO
# ──────────────────────────────────────────────────────────────

def fmt_num(valor: float, casas: int = 2) -> str:
    """
    Formata número com separador de milhar para exibição no log.

    O módulo logging não suporta o especificador '%,' nativamente.
    Esta função converte o valor para string formatada antes de logar.

    Args:
        valor: Número a formatar.
        casas: Casas decimais (padrão: 2).

    Returns:
        String formatada. Ex: 1696001.0 → '1,696,001.00'
    """
    return f"{valor:,.{casas}f}"


def fmt_milhoes(x, _):
    """Formata eixo do gráfico em K/M/B para melhor legibilidade."""
    if abs(x) >= 1e9:
        return f"{x/1e9:.1f}B"
    if abs(x) >= 1e6:
        return f"{x/1e6:.0f}M"
    if abs(x) >= 1e3:
        return f"{x/1e3:.0f}K"
    return f"{x:.0f}"


# ──────────────────────────────────────────────────────────────
#  CARGA
# ──────────────────────────────────────────────────────────────

def carregar() -> pd.DataFrame:
    """
    Lê o CSV mais recente da camada Raw e prepara para análise.

    Returns:
        DataFrame com coluna produto_label normalizada.

    Raises:
        SystemExit: se nenhum CSV for encontrado.
    """
    csvs = sorted(PASTA_CSV.glob("ibge_pam_*.csv"), reverse=True)
    if not csvs:
        log.error("Nenhum CSV encontrado em %s.", PASTA_CSV)
        log.error("Execute primeiro: python main.py")
        sys.exit(1)

    arquivo = csvs[0]
    log.info("Carregando: %s", arquivo.name)

    df = pd.read_csv(arquivo, encoding="utf-8-sig")
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce")
    df["produto_label"] = df["produto"].map(MAPA_PRODUTO).fillna(df["produto"])

    log.info("  %d linhas x %d colunas carregadas", len(df), len(df.columns))
    return df


# ──────────────────────────────────────────────────────────────
#  1. ESTATÍSTICAS DESCRITIVAS
# ──────────────────────────────────────────────────────────────

def estatisticas_descritivas(df: pd.DataFrame) -> None:
    """
    Calcula e exibe estatísticas descritivas das variáveis numéricas.

    Métricas: count, média, mediana, desvio padrão, mín, máx, Q1, Q3.

    Args:
        df: DataFrame com os dados.
    """
    log.info("=" * 60)
    log.info("1. ESTATISTICAS DESCRITIVAS")
    log.info("=" * 60)

    colunas = [
        "area_colhida_ha",
        "qtd_produzida",
        "rendimento_medio_kg_ha",
        "valor_producao_mil_reais",
    ]

    df_2023 = df[df["ano"] == 2023].dropna(subset=colunas, how="all")

    for col in colunas:
        serie = df_2023[col].dropna()
        if serie.empty:
            continue

        log.info("")
        log.info("  Coluna: %s", col)
        log.info("    Registros : %d",    serie.count())
        log.info("    Media     : %s",    fmt_num(serie.mean()))
        log.info("    Mediana   : %s",    fmt_num(serie.median()))
        log.info("    Desvio P. : %s",    fmt_num(serie.std()))
        log.info("    Minimo    : %s",    fmt_num(serie.min()))
        log.info("    Maximo    : %s",    fmt_num(serie.max()))
        log.info("    Q1        : %s",    fmt_num(serie.quantile(0.25)))
        log.info("    Q3        : %s",    fmt_num(serie.quantile(0.75)))

    log.info("")
    log.info("  Resumo completo (2023):")
    resumo = df_2023[colunas].describe().round(2)
    resumo.loc["median"] = df_2023[colunas].median().round(2)
    log.info("\n%s", resumo.to_string())


# ──────────────────────────────────────────────────────────────
#  2. DETECÇÃO DE OUTLIERS — MÉTODO IQR
# ──────────────────────────────────────────────────────────────

def detectar_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detecta outliers pelo método IQR (Interquartile Range).

    Critério: valor fora de [Q1 - 1.5×IQR, Q3 + 1.5×IQR] é outlier.

    Args:
        df: DataFrame com os dados.

    Returns:
        DataFrame com os outliers encontrados.
    """
    log.info("")
    log.info("=" * 60)
    log.info("2. DETECCAO DE OUTLIERS — METODO IQR")
    log.info("=" * 60)

    df_2023 = df[df["ano"] == 2023].dropna(subset=["qtd_produzida"])
    colunas = [
        "area_colhida_ha",
        "qtd_produzida",
        "rendimento_medio_kg_ha",
        "valor_producao_mil_reais",
    ]

    todos = []

    for col in colunas:
        serie = df_2023[col].dropna()
        Q1  = serie.quantile(0.25)
        Q3  = serie.quantile(0.75)
        IQR = Q3 - Q1
        lim_inf = Q1 - 1.5 * IQR
        lim_sup = Q3 + 1.5 * IQR

        outliers = df_2023[
            (df_2023[col] < lim_inf) | (df_2023[col] > lim_sup)
        ][["produto_label", col]].copy()
        outliers["coluna"] = col

        log.info("")
        log.info("  %s", col)
        log.info("    Q1=%s  Q3=%s  IQR=%s",
                 fmt_num(Q1, 0), fmt_num(Q3, 0), fmt_num(IQR, 0))
        log.info("    Limites: [%s , %s]",
                 fmt_num(lim_inf, 0), fmt_num(lim_sup, 0))

        if outliers.empty:
            log.info("    Outliers: nenhum")
        else:
            for _, row in outliers.iterrows():
                log.info("    OUTLIER >> %-10s = %s  (fora do limite)",
                         row["produto_label"], fmt_num(row[col], 0))
            todos.append(outliers)

    return pd.concat(todos, ignore_index=True) if todos else pd.DataFrame()


# ──────────────────────────────────────────────────────────────
#  3. GRÁFICOS
# ──────────────────────────────────────────────────────────────

def grafico_boxplot(df: pd.DataFrame, pasta: Path) -> Path:
    """
    Boxplot da quantidade produzida por produto (2023).

    Args:
        df:    DataFrame com os dados.
        pasta: Pasta de destino.

    Returns:
        Path do arquivo gerado.
    """
    df_2023 = df[df["ano"] == 2023].dropna(subset=["qtd_produzida"])

    fig, ax = plt.subplots(figsize=(12, 6))

    produtos = df_2023["produto_label"].tolist()
    dados_bp = [[v] for v in df_2023["qtd_produzida"].tolist()]

    bp = ax.boxplot(
        dados_bp,
        tick_labels=produtos,
        patch_artist=True,
        flierprops=dict(marker="o", markerfacecolor="#E91E63", markersize=8),
    )

    for patch, cor in zip(bp["boxes"], CORES):
        patch.set_facecolor(cor)
        patch.set_alpha(0.7)

    ax.set_title("Quantidade Produzida por Commodity — Brasil 2023")
    ax.set_xlabel("Commodity")
    ax.set_ylabel("Quantidade Produzida (Toneladas)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_milhoes))
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()

    arq = pasta / "01_boxplot_quantidade_produzida.png"
    fig.savefig(arq, bbox_inches="tight")
    plt.close(fig)
    log.info("  Boxplot salvo: %s", arq)
    return arq


def grafico_histograma(df: pd.DataFrame, pasta: Path) -> Path:
    """
    Barras do rendimento médio por produto com linhas de média e mediana.

    Args:
        df:    DataFrame com os dados.
        pasta: Pasta de destino.

    Returns:
        Path do arquivo gerado.
    """
    df_2023 = df[df["ano"] == 2023].dropna(
        subset=["rendimento_medio_kg_ha"]
    ).sort_values("rendimento_medio_kg_ha", ascending=False)

    fig, ax = plt.subplots(figsize=(11, 5))

    ax.bar(
        df_2023["produto_label"],
        df_2023["rendimento_medio_kg_ha"],
        color=CORES[:len(df_2023)],
        alpha=0.85,
        edgecolor="white",
        linewidth=0.8,
    )

    media   = df_2023["rendimento_medio_kg_ha"].mean()
    mediana = df_2023["rendimento_medio_kg_ha"].median()

    ax.axhline(media,   color="#F44336", linestyle="--", linewidth=1.5,
               label=f"Media: {media:,.0f} kg/ha")
    ax.axhline(mediana, color="#FF9800", linestyle=":",  linewidth=1.5,
               label=f"Mediana: {mediana:,.0f} kg/ha")

    ax.set_title("Rendimento Medio por Commodity — Brasil 2023")
    ax.set_xlabel("Commodity")
    ax.set_ylabel("Rendimento Medio (Kg/ha)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_milhoes))
    ax.legend(framealpha=0.9)
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()

    arq = pasta / "02_histograma_rendimento.png"
    fig.savefig(arq, bbox_inches="tight")
    plt.close(fig)
    log.info("  Histograma salvo: %s", arq)
    return arq


def grafico_scatter(df: pd.DataFrame, pasta: Path) -> Path:
    """
    Scatter: área colhida vs valor de produção com linha de tendência.

    Args:
        df:    DataFrame com os dados.
        pasta: Pasta de destino.

    Returns:
        Path do arquivo gerado.
    """
    df_2023 = df[df["ano"] == 2023].dropna(
        subset=["area_colhida_ha", "valor_producao_mil_reais"]
    )

    fig, ax = plt.subplots(figsize=(10, 6))

    for i, (_, row) in enumerate(df_2023.iterrows()):
        ax.scatter(
            row["area_colhida_ha"],
            row["valor_producao_mil_reais"],
            color=CORES[i % len(CORES)],
            s=120, zorder=3,
            label=row["produto_label"],
        )
        ax.annotate(
            row["produto_label"],
            (row["area_colhida_ha"], row["valor_producao_mil_reais"]),
            textcoords="offset points", xytext=(8, 4),
            fontsize=9, color="#333333",
        )

    # Linha de tendência
    x = df_2023["area_colhida_ha"].values
    y = df_2023["valor_producao_mil_reais"].values
    z = np.polyfit(x, y, 1)
    p = np.poly1d(z)
    x_line = np.linspace(x.min(), x.max(), 100)
    ax.plot(x_line, p(x_line), "--", color="#9E9E9E",
            linewidth=1.2, label="Tendencia linear")

    ax.set_title("Area Colhida vs Valor de Producao — Brasil 2023")
    ax.set_xlabel("Area Colhida (ha)")
    ax.set_ylabel("Valor de Producao (Mil R$)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(fmt_milhoes))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_milhoes))
    ax.legend(fontsize=8, loc="upper left", framealpha=0.9)
    plt.tight_layout()

    arq = pasta / "03_scatter_area_vs_valor.png"
    fig.savefig(arq, bbox_inches="tight")
    plt.close(fig)
    log.info("  Scatter salvo: %s", arq)
    return arq


def grafico_barras(df: pd.DataFrame, pasta: Path) -> Path:
    """
    Barras horizontais: valor de producao por produto (2023).

    Args:
        df:    DataFrame com os dados.
        pasta: Pasta de destino.

    Returns:
        Path do arquivo gerado.
    """
    df_2023 = (
        df[df["ano"] == 2023]
        .dropna(subset=["valor_producao_mil_reais"])
        .sort_values("valor_producao_mil_reais", ascending=True)
    )

    fig, ax = plt.subplots(figsize=(10, 6))

    bars = ax.barh(
        df_2023["produto_label"],
        df_2023["valor_producao_mil_reais"],
        color=CORES[:len(df_2023)],
        alpha=0.85,
        edgecolor="white",
        linewidth=0.8,
    )

    max_val = df_2023["valor_producao_mil_reais"].max()
    for bar, val in zip(bars, df_2023["valor_producao_mil_reais"]):
        ax.text(
            bar.get_width() + max_val * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"R$ {val/1e6:,.0f}M",
            va="center", fontsize=9, color="#333333",
        )

    ax.set_title("Valor de Producao por Commodity — Brasil 2023")
    ax.set_xlabel("Valor de Producao (Mil R$)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(fmt_milhoes))
    plt.tight_layout()

    arq = pasta / "04_barras_valor_producao_2023.png"
    fig.savefig(arq, bbox_inches="tight")
    plt.close(fig)
    log.info("  Barras salvo: %s", arq)
    return arq


# ──────────────────────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────────────────────

def main() -> None:
    """
    Orquestra a análise exploratória completa.

    Sequência:
        1. Carrega dados da camada Raw
        2. Estatísticas descritivas
        3. Detecção de outliers (IQR)
        4. Gráficos (boxplot, histograma, scatter, barras)
    """
    log.info("=" * 60)
    log.info("EDA — Analise Exploratoria | agro-data-pipeline")
    log.info("=" * 60)

    PASTA_GRAFICOS.mkdir(parents=True, exist_ok=True)

    df = carregar()

    estatisticas_descritivas(df)

    df_outliers = detectar_outliers(df)
    if not df_outliers.empty:
        log.info("")
        log.info("  Resumo de outliers encontrados:")
        for _, row in df_outliers.iterrows():
            log.info("    %-10s → %s", row["produto_label"], row["coluna"])

    log.info("")
    log.info("=" * 60)
    log.info("3. GRAFICOS")
    log.info("=" * 60)
    grafico_boxplot(df, PASTA_GRAFICOS)
    grafico_histograma(df, PASTA_GRAFICOS)
    grafico_scatter(df, PASTA_GRAFICOS)
    grafico_barras(df, PASTA_GRAFICOS)

    log.info("")
    log.info("=" * 60)
    log.info("EDA concluida. Graficos em: %s", PASTA_GRAFICOS.resolve())
    log.info("=" * 60)


if __name__ == "__main__":
    main()