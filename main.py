import argparse
import subprocess
import sys
import logging
from pathlib import Path
import time

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

def executar_pipeline():
    """Executa o pipeline completo de dados."""
    log.info("🚀 Iniciando pipeline de dados...")
    
    scripts = [
        "src/data_collection.py",
        "src/data_processing.py", 
        "src/data_analysis.py"
    ]
    
    for script in scripts:
        script_path = Path(script)
        if not script_path.exists():
            log.warning(f"⚠️ Script {script} não encontrado, pulando...")
            continue
            
        log.info(f"▶️ Executando {script}...")
        try:
            result = subprocess.run([sys.executable, script], check=True, capture_output=True, text=True)
            log.info(f"✅ {script} executado com sucesso")
        except subprocess.CalledProcessError as e:
            log.error(f"❌ Erro ao executar {script}: {e}")
            log.error(f"Saída do erro: {e.stderr}")
            return False
    
    log.info("🎉 Pipeline executado com sucesso!")
    return True

def executar_dashboard():
    """Executa o dashboard Streamlit."""
    dashboard_path = Path("app_dashboard.py")
    if not dashboard_path.exists():
        log.error("❌ Arquivo app_dashboard.py não encontrado")
        return
    
    log.info("📊 Iniciando dashboard...")
    subprocess.run([sys.executable, "-m", "streamlit", "run", "app_dashboard.py"])

def executar_eda():
    """Executa análise exploratória de dados."""
    eda_path = Path("src/eda.py")
    if not eda_path.exists():
        log.error("❌ Arquivo src/eda.py não encontrado")
        return
    
    log.info("📊 Executando análise exploratória...")
    subprocess.run([sys.executable, "src/eda.py"])

def verificar_status():
    """Verifica o status dos arquivos e dependências."""
    log.info("🔍 Verificando status do sistema...")
    
    arquivos_necessarios = [
        "src/data_collection.py",
        "src/data_processing.py",
        "src/data_analysis.py",
        "src/eda.py",
        "app_dashboard.py",
        "requirements.txt"
    ]
    
    print("\n📁 STATUS DOS ARQUIVOS:")
    print("-" * 40)
    
    for arquivo in arquivos_necessarios:
        path = Path(arquivo)
        status = "✅ Existe" if path.exists() else "❌ Não encontrado"
        print(f"{arquivo:<25} {status}")
    
    # Verificar dependências
    print("\n📦 DEPENDÊNCIAS:")
    print("-" * 40)
    
    dependencias = [
        "pandas", "numpy", "plotly", "streamlit", 
        "requests", "matplotlib", "seaborn"
    ]
    
    for dep in dependencias:
        try:
            __import__(dep)
            print(f"{dep:<15} ✅ Instalado")
        except ImportError:
            print(f"{dep:<15} ❌ Não instalado")
    
    print("\n" + "="*50)

def main():
    parser = argparse.ArgumentParser(
        description="🌾 Sistema Agrícola - Pipeline e Dashboard",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python main.py              # Executa pipeline padrão
  python main.py --dashboard  # Abre dashboard
  python main.py --eda        # Executa análise exploratória  
  python main.py --all        # Pipeline + Dashboard + EDA
  python main.py --status     # Verifica status do sistema
        """
    )
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--dashboard", 
        action="store_true", 
        help="Executa apenas o dashboard Streamlit"
    )
    group.add_argument(
        "--eda",
        action="store_true", 
        help="Executa análise exploratória (gráficos)"
    )
    group.add_argument(
        "--all", 
        action="store_true", 
        help="Executa pipeline + dashboard + EDA"
    )
    group.add_argument(
        "--status", 
        action="store_true", 
        help="Verifica status dos arquivos e dependências"
    )
    
    args = parser.parse_args()
    
    print("🌾" + "="*50 + "🌾")
    print("    SISTEMA AGRÍCOLA - PIPELINE E DASHBOARD")
    print("🌾" + "="*50 + "🌾")
    
    if args.dashboard:
        executar_dashboard()
    elif args.eda:
        executar_eda()
    elif args.all:
        log.info("🚀 Executando sistema completo...")
        if executar_pipeline():
            log.info("⏳ Aguardando 3 segundos...")
            time.sleep(3)
            executar_eda()
            log.info("⏳ Aguardando 2 segundos...")
            time.sleep(2)
            executar_dashboard()
    elif args.status:
        verificar_status()
    else:
        # Execução padrão - apenas pipeline
        executar_pipeline()

if __name__ == "__main__":
    main()