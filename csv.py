import pandas as pd
import os

# Definir diretórios
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Criar diretório se não existir
os.makedirs(DATA_DIR, exist_ok=True)

def processar_acessos(arquivo_csv, arquivo_mestre=os.path.join(DATA_DIR, "acessos_consolidados.csv")):
    arquivo_csv = os.path.join(DATA_DIR, arquivo_csv)  # Garante que a entrada esteja na pasta correta
    relatorio_saida = os.path.join(DATA_DIR, "relatorio_acessos.csv")  # Define o caminho do relatório

    # Definir colunas esperadas
    colunas = ["Usuario", "Credencial", "Codigo Cartao", "Nome Ponto de Acesso", "Dispositivo", "Data", "Detalhe", "Observacoes", "RG", "CPF", "Matricula", "Departamento", "Placa", "Modelo", "Cor", "Marca", "Status", "Sentido"]
    
    # Carregar o novo CSV
    df_novo = pd.read_csv(arquivo_csv, sep=';', names=colunas, skiprows=1, encoding='utf-8')
    
    # Filtrar apenas acessos válidos (excluir "Usuario Desconhecido")
    df_novo = df_novo[df_novo["Usuario"].str.lower() != "usuario desconhecido"]
    
    # Converter coluna de data para datetime
    df_novo["Data"] = pd.to_datetime(df_novo["Data"], errors='coerce')
    df_novo["Mes"] = df_novo["Data"].dt.strftime('%Y-%m')  # Extrair ano-mês
    df_novo["Dia"] = df_novo["Data"].dt.date  # Extrair apenas a data (ano-mês-dia)
    
    # Verificar se já existe um arquivo mestre
    if os.path.exists(arquivo_mestre):
        df_mestre = pd.read_csv(arquivo_mestre, sep=';', encoding='utf-8')
        df_mestre["Data"] = pd.to_datetime(df_mestre["Data"], errors='coerce')
    else:
        df_mestre = pd.DataFrame(columns=df_novo.columns)
    
    # Concatenar os novos registros com os antigos e remover duplicatas
    df_consolidado = pd.concat([df_mestre, df_novo]).drop_duplicates(subset=["Usuario", "Data"], keep="first")
    
    # Salvar o arquivo consolidado atualizado
    df_consolidado.to_csv(arquivo_mestre, index=False, sep=';', encoding='utf-8')
    
    # Contar acessos únicos por dia e cliente
    acessos_por_dia = df_consolidado.groupby(["Usuario", "Mes", "Dia"]).size().reset_index(name="Acessos Diarios")
    
    # Contar acessos totais por cliente no mês
    acessos_mensais = acessos_por_dia.groupby(["Usuario", "Mes"]).size().reset_index(name="Nº de Acessos")
    
    # Criar coluna "Controle" (SIM/NÃO para clientes com 6 ou mais acessos)
    acessos_mensais["Controle"] = acessos_mensais["Nº de Acessos"].apply(lambda x: "SIM" if x >= 6 else "NÃO")
    
    # Contar total de clientes com 6 ou mais acessos
    total_controle = acessos_mensais[acessos_mensais["Controle"] == "SIM"].shape[0]
    print(f"Total de clientes com 6 ou mais acessos no mês: {total_controle}")
    
    # Salvar o relatório de saída
    acessos_mensais.to_csv("relatorio_acessos.csv", index=False, sep=';', encoding='utf-8')
    print("Relatório gerado com sucesso: relatorio_acessos.csv")

# Exemplo de uso
# processar_acessos("acessos.csv")
