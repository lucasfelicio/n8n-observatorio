from pyspark.sql import functions as F

def transformation(spark, BRONZE_PATH: str, SILVER_PATH: str):

    BRONZE_TABLE = f"{BRONZE_PATH}/delta/acessos_banda_larga_fixa"
    SILVER_TABLE = f"{SILVER_PATH}/acessos"

    df = (spark.read
        .format("delta")
        .load(str(BRONZE_TABLE))
    )

    uf_to_regiao = {
        "AC":"Norte","AL":"Nordeste","AP":"Norte","AM":"Norte","BA":"Nordeste","CE":"Nordeste",
        "DF":"Centro-Oeste","ES":"Sudeste","GO":"Centro-Oeste","MA":"Nordeste","MT":"Centro-Oeste",
        "MS":"Centro-Oeste","MG":"Sudeste","PA":"Norte","PB":"Nordeste","PR":"Sul","PE":"Nordeste",
        "PI":"Nordeste","RJ":"Sudeste","RN":"Nordeste","RS":"Sul","RO":"Norte","RR":"Norte",
        "SC":"Sul","SP":"Sudeste","SE":"Nordeste","TO":"Norte"
    }

    mapping_rows = [(k,v) for k,v in uf_to_regiao.items()]
    df_mapping = spark.createDataFrame(mapping_rows, schema=["uf","regiao"])

    df.printSchema()

    df_result = (df
        .join(df_mapping, on="uf", how="left")
        .withColumn("velocidade", F.regexp_replace(F.col("velocidade"), ",", "."))
        .withColumn("velocidade", F.col("velocidade").cast("float"))
        .withColumn("ano", F.col("ano").cast("int"))
        .withColumn("mes", F.col("mes").cast("int"))
        .withColumn("acessos", F.col("acessos").cast("int"))
    )

    (df_result.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_TABLE)
    )