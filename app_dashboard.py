import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import numpy as np

# Configuração da página
st.set_page_config(
    page_title="Dashboard Agrícola Brasil",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS customizado
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
    .metric-card {
        background-color: #f0f8f0;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #2E8B57;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_ibge_data():
    """Carrega dados reais do IBGE"""
    try:
        # URL da API SIDRA - Produção Agrícola Municipal
        url = "https://apisidra.ibge.gov.br/values/t/1612/n6/all/v/214,216/p/2022/c81/2692,2693,2694,2695,2696,2697"
        
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            if len(data) > 1:
                df = pd.DataFrame(data[1:], columns=data[0])
                
                # Limpeza dos dados
                df = df[df['Valor'] != '..'].copy()
                df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
                df = df.dropna(subset=['Valor'])
                
                # Renomear colunas
                df = df.rename(columns={
                    'Produto das lavouras temporárias': 'produto',
                    'Variável': 'variavel',
                    'Valor': 'valor',
                    'Município': 'municipio'
                })
                
                return df
        
        # Se falhar, retorna dados de exemplo
        return create_sample_data()
        
    except Exception as e:
        st.warning(f"Erro ao carregar dados do IBGE: {e}. Usando dados de exemplo.")
        return create_sample_data()

def create_sample_data():
    """Cria dados de exemplo"""
    np.random.seed(42)
    
    produtos = ['Soja', 'Milho', 'Cana-de-açúcar', 'Café', 'Algodão', 'Arroz', 'Feijão', 'Trigo']
    variaveis = ['Quantidade produzida', 'Área plantada']
    
    data = []
    for produto in produtos:
        for variavel in variaveis:
            if variavel == 'Quantidade produzida':
                valor = np.random.randint(50000, 500000)
            else:  # Área plantada
                valor = np.random.randint(10000, 100000)
            
            data.append({
                'produto': produto,
                'variavel': variavel,
                'valor': valor,
                'municipio': 'Exemplo'
            })
    
    return pd.DataFrame(data)

def create_production_chart(df):
    """Gráfico de produção"""
    try:
        prod_df = df[df['variavel'] == 'Quantidade produzida'].copy()
        
        if prod_df.empty:
            return None
        
        prod_data = prod_df.groupby('produto')['valor'].sum().sort_values(ascending=False).head(10)
        
        fig = px.bar(
            x=prod_data.values,
            y=prod_data.index,
            orientation='h',
            title="🌾 Top 10 Produtos - Produção (Toneladas)",
            labels={'x': 'Produção (Toneladas)', 'y': 'Produto'},
            color=prod_data.values,
            color_continuous_scale='Greens'
        )
        
        fig.update_layout(
            height=500,
            showlegend=False,
            xaxis_title="Produção (Toneladas)",
            yaxis_title="Produto"
        )
        
        return fig
        
    except Exception as e:
        st.error(f"Erro no gráfico de produção: {e}")
        return None

def create_area_chart(df):
    """Gráfico de área plantada"""
    try:
        area_df = df[df['variavel'] == 'Área plantada'].copy()
        
        if area_df.empty:
            return None
        
        area_data = area_df.groupby('produto')['valor'].sum().sort_values(ascending=False).head(8)
        
        fig = px.pie(
            values=area_data.values,
            names=area_data.index,
            title="🗺️ Distribuição da Área Plantada (Hectares)"
        )
        
        fig.update_layout(height=500)
        
        return fig
        
    except Exception as e:
        st.error(f"Erro no gráfico de área: {e}")
        return None

def create_yield_chart(df):
    """Gráfico de rendimento"""
    try:
        prod_df = df[df['variavel'] == 'Quantidade produzida'].copy()
        area_df = df[df['variavel'] == 'Área plantada'].copy()
        
        if prod_df.empty or area_df.empty:
            return None
        
        prod_data = prod_df.groupby('produto')['valor'].sum()
        area_data = area_df.groupby('produto')['valor'].sum()
        
        yield_data = (prod_data / area_data).dropna().sort_values(ascending=False).head(8)
        
        fig = px.bar(
            x=yield_data.index,
            y=yield_data.values,
            title="📈 Rendimento Médio por Produto (Ton/Ha)",
            labels={'x': 'Produto', 'y': 'Rendimento (Ton/Ha)'},
            color=yield_data.values,
            color_continuous_scale='RdYlGn'
        )
        
        fig.update_xaxes(tickangle=45)
        fig.update_layout(
            height=500,
            showlegend=False
        )
        
        return fig
        
    except Exception as e:
        st.error(f"Erro no gráfico de rendimento: {e}")
        return None

def main():
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>🌾 Dashboard Agrícola Brasil</h1>
        <p>Análise de Produção Agrícola - Dados IBGE/SIDRA</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Carregar dados
    with st.spinner("🔄 Carregando dados do IBGE..."):
        df = load_ibge_data()
    
    if df.empty:
        st.error("❌ Não foi possível carregar os dados.")
        return
    
    # Calcular métricas
    prod_df = df[df['variavel'] == 'Quantidade produzida']
    area_df = df[df['variavel'] == 'Área plantada']
    
    total_producao = prod_df['valor'].sum() if not prod_df.empty else 0
    total_area = area_df['valor'].sum() if not area_df.empty else 0
    num_produtos = df['produto'].nunique()
    rendimento_medio = total_producao / total_area if total_area > 0 else 0
    
    # Métricas
    st.markdown("## 📊 Resumo Executivo")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "🌾 Produção Total",
            f"{total_producao:,.0f}",
            help="Total de toneladas produzidas"
        )
    
    with col2:
        st.metric(
            "🗺️ Área Total",
            f"{total_area:,.0f}",
            help="Total de hectares plantados"
        )
    
    with col3:
        st.metric(
            "🌱 Produtos",
            f"{num_produtos}",
            help="Número de produtos diferentes"
        )
    
    with col4:
        st.metric(
            "📈 Rendimento Médio",
            f"{rendimento_medio:.2f}",
            help="Toneladas por hectare"
        )
    
    # Gráficos em abas
    st.markdown("## 📈 Análises Detalhadas")
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Produção", 
        "🗺️ Área Plantada", 
        "📈 Rendimento", 
        "📋 Dados Brutos"
    ])
    
    with tab1:
        fig_prod = create_production_chart(df)
        if fig_prod:
            st.plotly_chart(fig_prod, use_container_width=True)
        else:
            st.warning("Dados de produção não disponíveis")
    
    with tab2:
        fig_area = create_area_chart(df)
        if fig_area:
            st.plotly_chart(fig_area, use_container_width=True)
        else:
            st.warning("Dados de área não disponíveis")
    
    with tab3:
        fig_yield = create_yield_chart(df)
        if fig_yield:
            st.plotly_chart(fig_yield, use_container_width=True)
        else:
            st.warning("Dados de rendimento não disponíveis")
    
    with tab4:
        st.markdown("### 📋 Dados Completos")
        st.dataframe(df, use_container_width=True)
        
        # Botão de download
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Baixar dados CSV",
            data=csv,
            file_name="dados_agricolas.csv",
            mime="text/csv"
        )

    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 1rem;'>
        🌾 Dashboard desenvolvido com Streamlit | 📊 Dados: IBGE/SIDRA<br>
        📧 Contato: elbiasimone@hotmail.com
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()