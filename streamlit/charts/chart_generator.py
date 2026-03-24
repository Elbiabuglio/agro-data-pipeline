import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st

class ChartGenerator:
    def __init__(self, df):
        self.df = df
    
    def create_production_chart(self):
        """Cria gráfico de produção por produto"""
        try:
            if self.df.empty or 'produto' not in self.df.columns:
                return None
            
            # Filtrar apenas dados de quantidade produzida
            prod_df = self.df[self.df['variavel'] == 'Quantidade produzida'].copy()
            
            if prod_df.empty:
                return None
            
            # Agrupar por produto
            prod_data = prod_df.groupby('produto')['valor'].sum().sort_values(ascending=False).head(10)
            
            fig = px.bar(
                x=prod_data.values,
                y=prod_data.index,
                orientation='h',
                title="Top 10 Produtos - Quantidade Produzida (Toneladas)",
                labels={'x': 'Quantidade (Toneladas)', 'y': 'Produto'},
                color=prod_data.values,
                color_continuous_scale='Viridis'
            )
            
            fig.update_layout(
                height=500,
                showlegend=False,
                xaxis_title="Quantidade (Toneladas)",
                yaxis_title="Produto"
            )
            
            return fig
            
        except Exception as e:
            st.error(f"Erro ao criar gráfico de produção: {e}")
            return None
    
    def create_area_chart(self):
        """Cria gráfico de área plantada"""
        try:
            if self.df.empty or 'produto' not in self.df.columns:
                return None
            
            # Filtrar apenas dados de área plantada
            area_df = self.df[self.df['variavel'] == 'Área plantada'].copy()
            
            if area_df.empty:
                return None
            
            # Agrupar por produto
            area_data = area_df.groupby('produto')['valor'].sum().sort_values(ascending=False).head(10)
            
            fig = px.pie(
                values=area_data.values,
                names=area_data.index,
                title="Distribuição - Área Plantada (Hectares)"
            )
            
            fig.update_layout(height=500)
            
            return fig
            
        except Exception as e:
            st.error(f"Erro ao criar gráfico de área: {e}")
            return None
    
    def create_yield_chart(self):
        """Cria gráfico de rendimento médio"""
        try:
            if self.df.empty or 'produto' not in self.df.columns:
                return None
            
            # Calcular rendimento (produção/área)
            prod_df = self.df[self.df['variavel'] == 'Quantidade produzida'].copy()
            area_df = self.df[self.df['variavel'] == 'Área plantada'].copy()
            
            if prod_df.empty or area_df.empty:
                return None
            
            # Agrupar dados
            prod_data = prod_df.groupby('produto')['valor'].sum()
            area_data = area_df.groupby('produto')['valor'].sum()
            
            # Calcular rendimento
            yield_data = (prod_data / area_data).dropna().sort_values(ascending=False).head(10)
            
            fig = px.bar(
                x=yield_data.index,
                y=yield_data.values,
                title="Rendimento Médio por Produto (Ton/Ha)",
                labels={'x': 'Produto', 'y': 'Rendimento (Ton/Ha)'},
                color=yield_data.values,
                color_continuous_scale='RdYlGn'
            )
            
            fig.update_xaxes(tickangle=45)
            fig.update_layout(
                height=500,
                showlegend=False,
                xaxis_title="Produto",
                yaxis_title="Rendimento (Ton/Ha)"
            )
            
            return fig
            
        except Exception as e:
            st.error(f"Erro ao criar gráfico de rendimento: {e}")
            return None
    
    def get_summary_metrics(self):
        """Retorna métricas resumidas"""
        try:
            if self.df.empty:
                return {
                    'total_producao': 0,
                    'total_area': 0,
                    'num_produtos': 0,
                    'rendimento_medio': 0
                }
            
            prod_df = self.df[self.df['variavel'] == 'Quantidade produzida']
            area_df = self.df[self.df['variavel'] == 'Área plantada']
            
            total_producao = prod_df['valor'].sum() if not prod_df.empty else 0
            total_area = area_df['valor'].sum() if not area_df.empty else 0
            num_produtos = self.df['produto'].nunique() if 'produto' in self.df.columns else 0
            rendimento_medio = total_producao / total_area if total_area > 0 else 0
            
            return {
                'total_producao': total_producao,
                'total_area': total_area,
                'num_produtos': num_produtos,
                'rendimento_medio': rendimento_medio
            }
            
        except Exception as e:
            st.error(f"Erro ao calcular métricas: {e}")
            return {
                'total_producao': 0,
                'total_area': 0,
                'num_produtos': 0,
                'rendimento_medio': 0
            }