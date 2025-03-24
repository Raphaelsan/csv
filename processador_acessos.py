import pandas as pd
import os
import tkinter as tk
from tkinter import filedialog
from ttkbootstrap import Style
from ttkbootstrap import ttk  # Importar ttk do ttkbootstrap para usar o estilo

# Definir diretórios
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Caminho correto para o ícone
favicon_path = os.path.join(BASE_DIR, "data", "flux_favicon_48x48.ico")

# Criar diretório se não existir
os.makedirs(DATA_DIR, exist_ok=True)

def processar_acessos(arquivo_csv, arquivo_mestre=os.path.join(DATA_DIR, "acessos_consolidados.csv")):
    arquivo_csv = os.path.join(DATA_DIR, arquivo_csv)  # Garante que a entrada esteja na pasta correta
    relatorio_saida = os.path.join(DATA_DIR, "relatorio_acessos.csv")  # Relatório mensal
    relatorio_dia_saida = os.path.join(DATA_DIR, "relatorio_acessos_dia.csv")  # Relatório por dia da semana

    # Definir colunas esperadas
    colunas = ["Usuario", "Credencial", "Codigo Cartao", "Nome Ponto de Acesso", "Dispositivo", "Data", "Detalhe", "Observacoes", "RG", "CPF", "Matricula", "Departamento", "Placa", "Modelo", "Cor", "Marca", "Status", "Sentido"]
    
    # Carregar o novo CSV
    df_novo = pd.read_csv(arquivo_csv, sep=';', names=colunas, skiprows=1, encoding='utf-8')

    # Verificar as primeiras linhas dos dados carregados
    print("Primeiras linhas dos dados carregados:")
    print(df_novo.head())

    # Filtrar apenas acessos válidos (excluir "Usuario Desconhecido")
    df_novo = df_novo[df_novo["Usuario"].str.lower() != "usuario desconhecido"]
    
    # Verificar se a coluna de "Data" existe
    if "Data" not in df_novo.columns:
        print("⚠️ A coluna 'Data' não foi encontrada no arquivo CSV.")
        return
    
    # Converter coluna de data para datetime (Corrigido: Forçar formato correto)
    try:
        df_novo["Data"] = pd.to_datetime(df_novo["Data"], errors='coerce', format="%d/%m/%Y %H:%M")
    except Exception as e:
        print(f"Erro ao converter as datas: {e}")
        return
    
    # Verificar se há valores nulos após a conversão
    print("Valores nulos na coluna 'Data' após conversão:", df_novo["Data"].isnull().sum())
    
    # Remover valores NaT da data (Se alguma data não foi convertida corretamente)
    df_novo = df_novo.dropna(subset=["Data"])

    # Verificar se existem dados após a limpeza
    if df_novo.empty:
        print("⚠️ Nenhum dado válido encontrado após a limpeza da coluna 'Data'.")
        return
    
    # Criar colunas auxiliares
    df_novo["Mes"] = df_novo["Data"].dt.strftime('%Y-%m')  # Ano-Mês
    df_novo["Dia"] = df_novo["Data"].dt.date  # Ano-Mês-Dia
    df_novo["Dia da Semana"] = df_novo["Data"].dt.strftime('%A')  # Nome do dia da semana

    # Verificar os primeiros dados processados
    print("Dados processados:")
    print(df_novo[["Usuario", "Data", "Dia da Semana"]].head())

    # Verificar se o arquivo mestre existe
    if os.path.exists(arquivo_mestre):
        df_mestre = pd.read_csv(arquivo_mestre, sep=';', encoding='utf-8')
        df_mestre["Data"] = pd.to_datetime(df_mestre["Data"], errors='coerce')
        df_mestre = df_mestre.dropna(subset=["Data"])  # Remover linhas inválidas
    else:
        df_mestre = pd.DataFrame(columns=df_novo.columns)
    
    # Concatenar os novos registros com os antigos e remover duplicatas
    df_consolidado = pd.concat([df_mestre, df_novo]).drop_duplicates(subset=["Usuario", "Data"], keep="first")
    
    # Salvar o arquivo consolidado atualizado
    df_consolidado.to_csv(arquivo_mestre, index=False, sep=';', encoding='utf-8')
    
    # Criar relatório mensal de acessos
    acessos_por_dia = df_consolidado.groupby(["Usuario", "Mes", "Dia"]).size().reset_index(name="Acessos Diarios")
    acessos_mensais = acessos_por_dia.groupby(["Usuario", "Mes"]).size().reset_index(name="Nº de Acessos")
    acessos_mensais["Controle"] = acessos_mensais["Nº de Acessos"].apply(lambda x: "SIM" if x >= 6 else "NÃO")

    # Salvar relatório mensal
    acessos_mensais.to_csv(relatorio_saida, index=False, sep=';', encoding='utf-8')

    print(f"Relatório de acessos mensais salvo: {relatorio_saida}")

    # 📌 NOVO: Contar acessos por usuário e dia da semana
    acessos_por_dia_semana = df_consolidado.groupby(["Usuario", "Dia da Semana"]).size().reset_index(name="Acessos Semana")
    
    if acessos_por_dia_semana.empty:
        print("⚠️ Relatório de acessos por dia da semana está vazio! Verifique os dados de entrada.")
    
    # Ordenar os dias da semana corretamente (Se estiver em inglês ou português)
    if "Monday" in acessos_por_dia_semana["Dia da Semana"].values:
        dias_ordenados = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    else:
        dias_ordenados = ["segunda-feira", "terça-feira", "quarta-feira", "quinta-feira", "sexta-feira", "sábado", "domingo"]

    acessos_por_dia_semana["Dia da Semana"] = pd.Categorical(acessos_por_dia_semana["Dia da Semana"], categories=dias_ordenados, ordered=True)
    acessos_por_dia_semana = acessos_por_dia_semana.sort_values(["Usuario", "Dia da Semana"])

    # Salvar relatório de acessos por dia da semana
    acessos_por_dia_semana.to_csv(relatorio_dia_saida, index=False, sep=';', encoding='utf-8')

    print(f"Relatório de acessos por dia da semana salvo: {relatorio_dia_saida}")


# Função para abrir o diálogo de seleção de arquivo
def selecionar_arquivo():
    arquivo_csv = filedialog.askopenfilename(title="Selecione o arquivo CSV", filetypes=[("CSV Files", "*.csv")])
    
    if arquivo_csv:
        processar_acessos(arquivo_csv)
        resultado_label.config(text="Arquivo processado com sucesso!")

# Criando a interface gráfica
def criar_interface():
    # Estilo ttkbootstrap
    style = Style(theme='superhero')

    # Janela principal
    root = style.master

    # Definir o tamanho da janela principal (largura x altura)
    root.geometry("250x150")  # Aumente os valores conforme necessário

    # Definir o favicon (ícone) da janela
    root.iconbitmap(favicon_path)  # Substitua pelo caminho correto do seu arquivo .ico


    # Título da janela
    root.title("Processador de Acessos Casa7")

    # Criar um estilo customizado para o botão
    style.configure('custom.TButton',
                    background="#fcc42c",  # Cor de fundo
                    padding=5,  # Espaçamento
                    font=("Helvetica", 12),  # Fonte
                    foreground="white",  # Cor do texto
                    focuscolor="none",  # Desabilitar foco para o botão
                    borderwidth=3,  # Tamanho da borda
                    relief="solid",  # Tipo de borda
                    bordercolor="#fcc42c")  # Cor da borda
    
    # Mapear o efeito de hover para o botão (quando o mouse passar sobre)
    style.map('custom.TButton',
              background=[('active', '#ffb84d'),  # Cor de fundo ao passar o mouse
                          ('!active', '#fcc42c')])  # Cor de fundo normal

    # Botão para selecionar o CSV com o estilo customizado
    selecionar_btn = ttk.Button(root, text="Selecionar CSV", command=selecionar_arquivo, style="custom.TButton")
    selecionar_btn.pack(pady=20)


    # Label para mostrar o resultado
    global resultado_label
    resultado_label = ttk.Label(root, text="", style="info.TLabel")
    resultado_label.pack(pady=10)

    # Iniciar a interface
    root.mainloop()

# Executar a interface
if __name__ == "__main__":
    criar_interface()