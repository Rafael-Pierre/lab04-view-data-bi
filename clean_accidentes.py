import pandas as pd
import numpy as np
import os

def clean_accidents(input_path, output_path="clean_accidents.csv", chunk_size=500000):
    print("🚀 Iniciando limpeza do dataset de acidentes...")
    print(f"Arquivo de entrada: {input_path}")
    print(f"Arquivo de saída: {output_path}")
    print(f"Processando em chunks de {chunk_size} linhas...\n")

    # Configuração inicial
    chunks = []
    total_rows = 0
    chunk_number = 1

    for chunk in pd.read_csv(input_path, chunksize=chunk_size, low_memory=False):
        print(f"🔹 Processando chunk {chunk_number}... ({len(chunk)} linhas)")
        total_rows += len(chunk)

        # Normaliza nomes de colunas
        chunk.columns = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in chunk.columns]

        # Converte data
        if 'data_inversa' in chunk.columns:
            chunk['data_inversa'] = pd.to_datetime(chunk['data_inversa'], errors='coerce')

        # Extrai ano/mês/dia
        if 'data_inversa' in chunk.columns:
            chunk['year'] = chunk['data_inversa'].dt.year
            chunk['month'] = chunk['data_inversa'].dt.month
            chunk['day'] = chunk['data_inversa'].dt.day

        # Extrai hora
        if 'horario' in chunk.columns:
            chunk['hour'] = pd.to_datetime(chunk['horario'], errors='coerce').dt.hour

        # Mês/Ano concatenado
        if 'data_inversa' in chunk.columns:
            chunk['month_year'] = chunk['data_inversa'].dt.to_period('M').astype(str)

        # Corrige colunas de texto (UF, município, tipo de acidente)
        for col in ['uf', 'municipio', 'tipo_acidente', 'classificacao_acidente']:
            if col in chunk.columns:
                chunk[col] = chunk[col].astype(str).str.strip().str.upper()

        # Cria flag para acidentes fatais
        if 'mortos' in chunk.columns:
            chunk['fatal_accident'] = chunk['mortos'].fillna(0).apply(lambda x: 1 if x > 0 else 0)

        # Converte colunas numéricas comuns
        numeric_cols = ['ilesos', 'feridos_leves', 'feridos_graves', 'mortos']
        for col in numeric_cols:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce').fillna(0).astype(int)

        # Remove linhas sem data
        chunk = chunk.dropna(subset=['data_inversa'])

        # Armazena chunk limpo
        chunks.append(chunk)
        print(f"✅ Chunk {chunk_number} limpo e armazenado.\n")
        chunk_number += 1

    # Junta todos os chunks
    print("🔄 Concatenando todos os chunks...")
    df_clean = pd.concat(chunks, ignore_index=True)
    print(f"✅ Total de linhas processadas: {total_rows:,}")

    # Salva arquivo limpo
    print(f"💾 Salvando arquivo final em: {output_path}")
    df_clean.to_csv(output_path, index=False, encoding='utf-8-sig')

    print("\n🎉 Limpeza concluída com sucesso!")
    print(f"Arquivo limpo disponível em: {os.path.abspath(output_path)}")


# -----------------------
# Uso do script:
# -----------------------
# Altere o caminho abaixo para o seu arquivo original de acidentes.
# Exemplo:
# clean_accidents("acidentes_brasil_2017_2023.csv")

if __name__ == "__main__":
    caminho_arquivo = "accidents_2017_to_2023_portugues.csv"  # altere aqui!
    clean_accidents(caminho_arquivo)
