import requests
import zipfile
import os
import io

from pyspark.sql import DataFrame
from typing import List
from delta.tables import DeltaTable

col_map = {
    "Ano": "ano",
    "Mês": "mes",
    "Grupo Econômico": "grupo_economico",
    "Empresa": "empresa",
    "CNPJ": "cnpj",
    "Porte da Prestadora": "porte_prestadora",
    "UF": "uf",
    "Município": "municipio",
    "Código IBGE Município": "codigo_ibge",
    "Faixa de Velocidade": "faixa_velocidade",
    "Velocidade": "velocidade",
    "Tecnologia": "tecnologia",
    "Meio de Acesso": "meio_de_acesso",
    "Tipo de Pessoa": "tipo_pessoa",
    "Tipo de Produto": "tipo_produto",
    "Acessos": "acessos",
}

def download_zip(url: str) -> zipfile.ZipFile:
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()
    return zipfile.ZipFile(io.BytesIO(response.content))

def list_csv_files(zip_ref: zipfile.ZipFile) -> List[str]:
    return [f for f in zip_ref.namelist() if f.lower().endswith(".csv")]

def filter_csv_files(csv_files: List[str]) -> List[str]:

    selected = []
    for f in csv_files:
        base = os.path.basename(f)
        if base.startswith("Acessos_Banda_Larga_Fixa_") and \
           "_colunas" not in base.lower() and \
           "total" not in base.lower():
            selected.append(f)
    return selected

def extract_csvs(BRONZE_PATH: str, URL: str) -> bool:
    try:
        path_landing = f"{BRONZE_PATH}/landing"
        zip_ref = download_zip(URL)
        csv_files = list_csv_files(zip_ref)
        csv_files_to_extract = filter_csv_files(csv_files)

        # Para testes
        csv_files_to_extract = ["Acessos_Banda_Larga_Fixa_2023.csv", "Acessos_Banda_Larga_Fixa_2024.csv", "Acessos_Banda_Larga_Fixa_2025.csv"]
        
        os.makedirs(path_landing, exist_ok=True)

        saved_files = []
        for csv_file in csv_files_to_extract:
            base = os.path.basename(csv_file)

            out_path = os.path.join(path_landing, base)

            with zip_ref.open(csv_file) as fh, open(out_path, "wb") as out:
                out.write(fh.read())  

            saved_files.append(out_path)

        return True if saved_files else False
    
    except Exception as e:
        print(f"Error during extraction: {e}")
        return False    

    
def normalize_columns(df: DataFrame) -> DataFrame:
    new_names = []
    for c in df.columns:
        if c in col_map:
            new_names.append((c, col_map[c]))
        else:
            safe = c.strip().lower().replace(" ", "_").replace("-", "_")
            safe = safe.replace("ç", "c").replace("ã", "a").replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ô","o").replace("ú","u").replace("ê","e")
            new_names.append((c, safe))
    for orig, new in new_names:
        df = df.withColumnRenamed(orig, new)
    return df

def ingestion(spark, BRONZE_PATH: str) -> bool:
    bronze_delta_path = f"{BRONZE_PATH}/delta/acessos_banda_larga_fixa"
    landing_path = f"{BRONZE_PATH}/landing"

    df = (spark.read
        .option("header", "true")
        .option("delimiter", ";")
        .option("inferSchema", "false")
        .csv(f"{landing_path}/")
    )

    df = normalize_columns(df)

    df.printSchema()

    (df.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .save(bronze_delta_path))


    return True
