import os

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pathlib import Path

from .bronze import extract_csvs, ingestion
from .silver import transformation
from .gold import create_dimensions, create_fact

URL = "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_banda_larga_fixa.zip"

SRC_ROOT = Path(os.getenv("DATA_LAKE", "/datalake"))
BRONZE_PATH = f"{SRC_ROOT}/bronze"
SILVER_PATH = f"{SRC_ROOT}/silver"
GOLD_PATH = f"{SRC_ROOT}/gold"

builder = SparkSession.builder \
    .appName("IngestionData") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

print("Starting ETL process...")
print("Extracting data from source")
response = extract_csvs(BRONZE_PATH, URL)
print("Data extracted successfully")

if response:
    print("Starting data ingestion")
    ingestion(spark, BRONZE_PATH)
    print("Data ingested successfully")
    print("Starting data transformation")
    transformation(spark, BRONZE_PATH, SILVER_PATH)
    print("Data transformed successfully")
    print("Creating dimensions and fact tables")
    create_dimensions(spark, SILVER_PATH, GOLD_PATH)
    print("Dimensions created successfully")
    print("Creating fact table")
    create_fact(spark, SILVER_PATH, GOLD_PATH)
    print("Fact table created successfully")

print("ETL process completed successfully")

spark.stop()