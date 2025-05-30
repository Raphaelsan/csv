import pandas as pd
import os

# Definir diretórios
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Criar diretório se não existir
os.makedirs(DATA_DIR, exist_ok=True)
print(f"Diretório de dados: {DATA_DIR}")

def processar_acessos(arquivo_csv, arquivo_mestre=os.path.join(DATA_DIR, "acessos_consolidados.csv")):
    print(f"Processando arquivo: {arquivo_csv}")
    arquivo_csv = os.path.join(DATA_DIR, arquivo_csv)  # Garante que a entrada esteja na pasta correta
    relatorio_saida = os.path.join(DATA_DIR, "relatorio_acessos.csv")  # Define o caminho do relatório
    relatorio_dia_saida = os.path.join(DATA_DIR, "relatorio_acessos_dia.csv")  # Novo relatório por dia da semana

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
    df_novo["Dia da Semana"] = df_novo["Data"].dt.day_name()  # Novo: Extrair nome do dia da semana
    
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
    acessos_mensais = acessos_por_dia.groupby(["Usuario", "Mes"]).size().reset_index(name="Num de Acessos(MES)")
    
    # Criar coluna "Controle" (SIM/NÃO para clientes com 6 ou mais acessos)
    acessos_mensais["Controle"] = acessos_mensais["Num de Acessos(MES)"].apply(lambda x: "SIM" if x >= 6 else "NAO")
    
    # Contar total de clientes com 6 ou mais acessos
    total_controle = acessos_mensais[acessos_mensais["Controle"] == "SIM"].shape[0]
    print(f"Total de clientes com 6 ou mais acessos no mês: {total_controle}")
    
    # Salvar o relatório de saída
    acessos_mensais.to_csv(relatorio_saida, index=False, sep=';', encoding='utf-8')
    print(f"Relatório gerado com sucesso: {relatorio_saida}")

    # NOVO(parte ligação tarik): Contar acessos por usuário e dia da semana
    acessos_por_dia_semana = df_consolidado.groupby(["Usuario", "Dia da Semana"]).size().reset_index(name="Acessos Semana")
    
    # Ordenar os dias da semana corretamente
    dias_ordenados = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    acessos_por_dia_semana["Dia da Semana"] = pd.Categorical(acessos_por_dia_semana["Dia da Semana"], categories=dias_ordenados, ordered=True)
    acessos_por_dia_semana = acessos_por_dia_semana.sort_values(["Usuario", "Dia da Semana"])

    # Salvar relatório por dia da semana
    acessos_por_dia_semana.to_csv(relatorio_dia_saida, index=False, sep=';', encoding='utf-8')
    print(f"Relatório de acessos por dia da semana gerado com sucesso: {relatorio_dia_saida}")

    # Gerar lista de usuários únicos com a data do último acesso
    usuarios_unicos = (
        df_consolidado
        .sort_values("Data")  # Ordena pela data
        .dropna(subset=["Usuario", "Data"])  # Garante que só considera registros válidos
        .groupby("Usuario")
        .agg({"Data": "max"})  # Último acesso
        .reset_index()
        .rename(columns={"Data": "Último Acesso"})
    )

    caminho_usuarios = os.path.join(DATA_DIR, "usuarios_unicos.csv")
    usuarios_unicos.to_csv(caminho_usuarios, index=False, sep=';', encoding='utf-8')
    print(f"📋 Lista de usuários únicos gerada: {caminho_usuarios}")



# Iniciar o processamento
if __name__ == "__main__":
    processar_acessos("acessos2.csv")
