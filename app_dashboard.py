"""
app_dashboard.py
================
Dashboard Streamlit — Produção Agrícola Brasil
Fonte: IBGE/SIDRA — PAM Tabela 5457

Execução local:
    streamlit run app_dashboard.py

Deploy:
    Streamlit Cloud → https://share.streamlit.io
"""

import requests
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from pathlib import Path

# ──────────────────────────────────────────────────────────────
#  CONFIGURAÇÃO DA PÁGINA
# ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Dashboard Agrícola Brasil",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #2E8B57 0%, #228B22 100%);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
    }
    .fonte-nota {
        font-size: 0.8rem;
        color: #666;
        text-align: right;
        margin-top: -1rem;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────
#  CARGA DE DADOS
# ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def carregar_dados() -> pd.DataFrame:
    """
    Tenta carregar dados na seguinte ordem de prioridade:
        1. CSV local da camada processed (data/processed/)
        2. API IBGE/SIDRA diretamente (Tabela 5457)
        3. Dados de demonstração (fallback)
    """

    # 1. CSV local (camada processed)
    pasta = Path("data/processed")
    csvs = sorted(pasta.glob("ibge_pam_processed_*.csv"), reverse=True) if pasta.exists() else []
    if csvs:
        df = pd.read_csv(csvs[0], encoding="utf-8-sig")
        df["fonte_carga"] = "CSV local (processed)"
        return df

    # 2. API IBGE/SIDRA — Tabela 5457, nível Brasil, anos 2023 e 2024
    try:
        url = (
            "https://apisidra.ibge.gov.br/values/t/5457/n1/all"
            "/v/214,215,112,216"
            "/p/2023,2024"
            "/c782/40124,40125,40126,40128,40131,40132,40133,40135,40138"
        )
        r = requests.get(url, timeout=20)
        if r.status_code == 200:
            rows = r.json()
            if len(rows) > 1:
                cabecalho = rows[0]
                dados = rows[1:]

                registros = []
                for row in dados:
                    val = row.get("V", "")
                    try:
                        valor = float(val.replace(".", "").replace(",", ".")) if val not in ("-", "...", "X", "") else None
                    except ValueError:
                        valor = None

                    var_cod = row.get("D2C", "")
                    col_map = {
                        "214": "area_colhida_ha",
                        "215": "qtd_produzida",
                        "112": "rendimento_medio_kg_ha",
                        "216": "valor_producao_mil_reais",
                    }
                    mapa_produto = {
                        "40124": "Soja",    "40125": "Milho",
                        "40126": "Algodão", "40128": "Arroz",
                        "40131": "Feijão",  "40132": "Café",
                        "40133": "Trigo",   "40135": "Cana",
                        "40138": "Mandioca",
                    }

                    registros.append({
                        "produto"                 : mapa_produto.get(row.get("D4C", ""), row.get("D4N", "")),
                        "produto_cod"             : row.get("D4C", ""),
                        "ano"                     : int(row.get("D3N", 0)),
                        "variavel"                : col_map.get(var_cod, var_cod),
                        "valor"                   : valor,
                        "nivel_territorial"       : row.get("NN", ""),
                        "nome_territorial"        : row.get("D1N", ""),
                        "fonte_carga"             : "API IBGE/SIDRA",
                    })

                # Pivota para formato largo
                df_long = pd.DataFrame(registros)
                df = df_long.pivot_table(
                    index=["produto", "produto_cod", "ano", "nivel_territorial", "nome_territorial", "fonte_carga"],
                    columns="variavel",
                    values="valor",
                    aggfunc="first",
                ).reset_index()
                df.columns.name = None
                return df

    except Exception:
        pass

    # 3. Dados de demonstração
    np.random.seed(42)
    produtos = ["Soja", "Milho", "Cana", "Café", "Algodão", "Arroz", "Feijão", "Trigo"]
    anos = [2023, 2024]
    base = {"Soja": (45e6, 162e6, 3604, 245e6), "Milho": (22e6, 137e6, 6026, 77e6),
            "Cana": (8.6e6, 672e6, 78043, 79e6), "Café": (2.2e6, 3.5e6, 1621, 49e6),
            "Algodão": (1.7e6, 6.9e6, 4096, 28e6), "Arroz": (1.7e6, 10.6e6, 6248, 7.6e6),
            "Feijão": (2.9e6, 2.9e6, 997, 18e6), "Trigo": (2.8e6, 9.9e6, 3543, 8.9e6)}
    rows = []
    for p in produtos:
        for ano in anos:
            v = base[p]
            rows.append({
                "produto": p, "produto_cod": "0", "ano": ano,
                "nivel_territorial": "Brasil", "nome_territorial": "Brasil",
                "area_colhida_ha": v[0] * np.random.uniform(0.95, 1.05),
                "qtd_produzida": v[1] * np.random.uniform(0.95, 1.05),
                "rendimento_medio_kg_ha": v[2] * np.random.uniform(0.95, 1.05),
                "valor_producao_mil_reais": v[3] * np.random.uniform(0.95, 1.05),
                "fonte_carga": "Demonstração",
            })
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────────────────────
#  SIDEBAR
# ──────────────────────────────────────────────────────────────

def sidebar(df: pd.DataFrame):
    """Filtros na barra lateral."""
    st.sidebar.header("🔍 Filtros")

    anos_disp = sorted(df["ano"].dropna().unique().tolist(), reverse=True)
    ano_sel = st.sidebar.multiselect(
        "Ano", anos_disp, default=anos_disp[:1]
    )

    produtos_disp = sorted(df["produto"].dropna().unique().tolist())
    prod_sel = st.sidebar.multiselect(
        "Commodity", produtos_disp, default=produtos_disp
    )

    st.sidebar.markdown("---")
    st.sidebar.caption(f"Fonte: {df['fonte_carga'].iloc[0]}")
    st.sidebar.caption("Tabela SIDRA 5457 — PAM/IBGE")

    return ano_sel, prod_sel


# ──────────────────────────────────────────────────────────────
#  GRÁFICOS
# ──────────────────────────────────────────────────────────────

def grafico_barras_producao(df: pd.DataFrame):
    """Barras horizontais — quantidade produzida por produto."""
    dados = (
        df.dropna(subset=["qtd_produzida"])
        .groupby("produto")["qtd_produzida"]
        .sum()
        .sort_values()
        .reset_index()
    )
    fig = px.bar(
        dados, x="qtd_produzida", y="produto", orientation="h",
        title="Quantidade Produzida por Commodity (Toneladas)",
        labels={"qtd_produzida": "Toneladas", "produto": ""},
        color="qtd_produzida", color_continuous_scale="Greens",
    )
    fig.update_layout(height=420, showlegend=False, coloraxis_showscale=False)
    return fig


def grafico_pizza_area(df: pd.DataFrame):
    """Pizza — distribuição da área colhida."""
    dados = (
        df.dropna(subset=["area_colhida_ha"])
        .groupby("produto")["area_colhida_ha"]
        .sum()
        .reset_index()
    )
    fig = px.pie(
        dados, values="area_colhida_ha", names="produto",
        title="Distribuição da Área Colhida (Hectares)",
        color_discrete_sequence=px.colors.sequential.Greens_r,
    )
    fig.update_layout(height=420)
    return fig


def grafico_rendimento(df: pd.DataFrame):
    """Barras — rendimento médio por produto."""
    dados = (
        df.dropna(subset=["rendimento_medio_kg_ha"])
        .groupby("produto")["rendimento_medio_kg_ha"]
        .mean()
        .sort_values(ascending=False)
        .reset_index()
    )
    media   = dados["rendimento_medio_kg_ha"].mean()
    mediana = dados["rendimento_medio_kg_ha"].median()

    fig = px.bar(
        dados, x="produto", y="rendimento_medio_kg_ha",
        title="Rendimento Médio por Commodity (Kg/ha)",
        labels={"rendimento_medio_kg_ha": "Kg/ha", "produto": ""},
        color="rendimento_medio_kg_ha", color_continuous_scale="RdYlGn",
    )
    fig.add_hline(y=media,   line_dash="dash", line_color="#E53935",
                  annotation_text=f"Média: {media:,.0f}")
    fig.add_hline(y=mediana, line_dash="dot",  line_color="#FB8C00",
                  annotation_text=f"Mediana: {mediana:,.0f}")
    fig.update_layout(height=420, showlegend=False, coloraxis_showscale=False)
    fig.update_xaxes(tickangle=30)
    return fig


def grafico_scatter(df: pd.DataFrame):
    """Scatter — área colhida vs valor de produção."""
    dados = (
        df.dropna(subset=["area_colhida_ha", "valor_producao_mil_reais"])
        .groupby("produto")[["area_colhida_ha", "valor_producao_mil_reais"]]
        .sum()
        .reset_index()
    )
    fig = px.scatter(
        dados, x="area_colhida_ha", y="valor_producao_mil_reais",
        text="produto", size="valor_producao_mil_reais",
        title="Área Colhida vs Valor de Produção",
        labels={
            "area_colhida_ha"         : "Área Colhida (ha)",
            "valor_producao_mil_reais": "Valor (Mil R$)",
        },
        color="produto",
    )

    # Linha de tendência manual com numpy (sem statsmodels)
    if len(dados) >= 2:
        x = dados["area_colhida_ha"].values
        y = dados["valor_producao_mil_reais"].values
        z = np.polyfit(x, y, 1)
        p = np.poly1d(z)
        x_line = np.linspace(x.min(), x.max(), 100)
        fig.add_scatter(
            x=x_line, y=p(x_line),
            mode="lines",
            line=dict(color="#9E9E9E", dash="dash", width=1.5),
            name="Tendência",
            showlegend=False,
        )

    fig.update_traces(textposition="top center")
    fig.update_layout(height=420, showlegend=False)
    return fig


# ──────────────────────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────────────────────

def main():
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>🌾 Dashboard Agrícola Brasil</h1>
        <p>Análise de Produção Agrícola — IBGE/SIDRA · PAM Tabela 5457</p>
    </div>
    """, unsafe_allow_html=True)

    # Carga
    with st.spinner("Carregando dados do IBGE/SIDRA..."):
        df_full = carregar_dados()

    if df_full.empty:
        st.error("Não foi possível carregar os dados.")
        return

    # Filtros
    ano_sel, prod_sel = sidebar(df_full)

    df = df_full.copy()
    if ano_sel:
        df = df[df["ano"].isin(ano_sel)]
    if prod_sel:
        df = df[df["produto"].isin(prod_sel)]

    if df.empty:
        st.warning("Nenhum dado para os filtros selecionados.")
        return

    # ── Métricas ──
    st.subheader("📊 Resumo Executivo")
    col1, col2, col3, col4 = st.columns(4)

    total_prod  = df["qtd_produzida"].sum()             if "qtd_produzida" in df else 0
    total_area  = df["area_colhida_ha"].sum()           if "area_colhida_ha" in df else 0
    total_valor = df["valor_producao_mil_reais"].sum()  if "valor_producao_mil_reais" in df else 0
    n_produtos  = df["produto"].nunique()
    rendimento  = total_prod / total_area if total_area > 0 else 0

    col1.metric("🌾 Quantidade Produzida", f"{total_prod/1e6:,.1f}M ton")
    col2.metric("🗺️ Área Colhida",         f"{total_area/1e6:,.1f}M ha")
    col3.metric("💰 Valor de Produção",    f"R$ {total_valor/1e6:,.0f}M")
    col4.metric("📈 Rendimento Médio",     f"{rendimento:,.0f} kg/ha")

    st.caption(f"Fonte: {df_full['fonte_carga'].iloc[0]} | Anos selecionados: {ano_sel}")

    # ── Gráficos ──
    st.subheader("📈 Análises Detalhadas")

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Produção",
        "🗺️ Área Colhida",
        "📈 Rendimento",
        "🔍 Correlação",
        "📋 Dados Brutos",
    ])

    with tab1:
        st.plotly_chart(grafico_barras_producao(df), use_container_width=True)

    with tab2:
        st.plotly_chart(grafico_pizza_area(df), use_container_width=True)

    with tab3:
        st.plotly_chart(grafico_rendimento(df), use_container_width=True)

    with tab4:
        st.plotly_chart(grafico_scatter(df), use_container_width=True)

    with tab5:
        st.dataframe(df, use_container_width=True)
        csv = df.to_csv(index=False, encoding="utf-8-sig")
        st.download_button(
            "📥 Baixar CSV",
            data=csv,
            file_name="dados_agricolas.csv",
            mime="text/csv",
        )

    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align:center;color:#666;padding:1rem;font-size:0.85rem;'>
        🌾 Dashboard Agrícola Brasil · Dados: IBGE/SIDRA PAM Tabela 5457<br>
        Desenvolvido com Streamlit e Plotly
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()