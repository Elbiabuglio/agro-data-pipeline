#!/usr/bin/env python3
"""
Script para executar o dashboard Streamlit
"""
import subprocess
import sys
import os

def run_dashboard():
    """Executa o dashboard Streamlit"""
    
    # Verificar se está no diretório correto
    if not os.path.exists('streamlit/app.py'):
        print("❌ Erro: Execute este script a partir do diretório raiz do projeto")
        sys.exit(1)
    
    print("🚀 Iniciando Agro Data Pipeline Dashboard...")
    print("📊 Dashboard será aberto em: http://localhost:8501")
    print("⏹️  Para parar: Ctrl+C")
    print("-" * 50)
    
    try:
        # Executar Streamlit
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            "streamlit/app.py",
            "--server.port=8501",
            "--server.address=localhost"
        ])
    except KeyboardInterrupt:
        print("\n🛑 Dashboard interrompido pelo usuário")
    except Exception as e:
        print(f"❌ Erro ao executar dashboard: {str(e)}")

if __name__ == "__main__":
    run_dashboard()