"""Módulo de extração de dados"""

import requests
import zipfile
import io
import os
from typing import List
from datetime import datetime

URL = "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_banda_larga_fixa.zip"
BASE_VOLUME_PATH = "/Volumes/landing_zone/gov_br/banda_larga"  # ajuste se necessário
CURRENT_YEAR = datetime.now().strftime("%Y")

FIXED_FILES = {
    "Densidade_Banda_Larga_Fixa.csv",
    "Acessos_Banda_Larga_Fixa_Total.csv"
}

def download_zip() -> zipfile.ZipFile:
    response = requests.get(URL, stream=True, timeout=120)
    response.raise_for_status()
    return zipfile.ZipFile(io.BytesIO(response.content))

def list_csv_files(zip_ref: zipfile.ZipFile) -> List[str]:
    return [f for f in zip_ref.namelist() if f.lower().endswith(".csv")]

def filter_csv_files(csv_files: List[str]) -> List[str]:
    selected = []
    for f in csv_files:
        base = os.path.basename(f)
        if base == f"Banda_Larga_Fixa_{CURRENT_YEAR}.csv":
            selected.append(f)
        elif base in FIXED_FILES:
            selected.append(f)
    return selected

def extract_csvs(zip_ref: zipfile.ZipFile, csv_files_to_extract: List[str]) -> List[str]:
    year_dir = os.path.join(BASE_VOLUME_PATH, CURRENT_YEAR)
    fixed_dir = os.path.join(BASE_VOLUME_PATH, "fixos")
    os.makedirs(year_dir, exist_ok=True)
    os.makedirs(fixed_dir, exist_ok=True)

    saved_files = []
    for csv_file in csv_files_to_extract:
        base = os.path.basename(csv_file)

        if base in FIXED_FILES:
            dest_dir = fixed_dir
        else:
            dest_dir = year_dir

        out_path = os.path.join(dest_dir, base)

        with zip_ref.open(csv_file) as fh, open(out_path, "wb") as out:
            out.write(fh.read())  

        saved_files.append(out_path)

    return saved_files

def main() -> List[str]:
    zip_ref = download_zip()
    csv_files = list_csv_files(zip_ref)
    filtered = filter_csv_files(csv_files)
    if not filtered:
        print("Nenhum arquivo encontrado para processar.")
        return []
    return extract_csvs(zip_ref, filtered)

if __name__ == "__main__":
    saved_files = main()
    print("Arquivos salvos:", saved_files)
