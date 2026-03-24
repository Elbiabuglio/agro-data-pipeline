import pandas as pd
import requests
import streamlit as st

class DataLoader:
    def __init__(self):
        self.base_url = "https://apisidra.ibge.gov.br/values"
    
    def load_production_data(self):
        """Carrega dados de produção agrícola"""
        try:
            # Tabela 1612 - Produção Agrícola Municipal
            url = f"{self.base_url}/t/1612/n6/all/v/214,216/p/2022/c81/2692,2693,2694,2695,2696"
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                df = pd.DataFrame(data[1:], columns=data[0])
                
                # Limpeza dos dados
                df = df[df['Valor'] != '..']
                df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
                df = df.dropna(subset=['Valor'])
                
                return df
            else:
                st.error("Erro ao carregar dados da API IBGE")
                return pd.DataFrame()
                
        except Exception as e:
            st.error(f"Erro ao processar dados: {e}")
            return pd.DataFrame()
    
    def process_data(self, df):
        """Processa e limpa os dados"""
        if df.empty:
            return df
        
        # Renomear colunas para facilitar uso
        column_mapping = {
            'Produto das lavouras temporárias': 'produto',
            'Variável': 'variavel',
            'Valor': 'valor',
            'Município': 'municipio'
        }
        
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.rename(columns={old_name: new_name})
        
        return df