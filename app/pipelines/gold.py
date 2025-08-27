from pyspark.sql import functions as F

def create_dimensions(spark, SILVER_PATH: str, GOLD_PATH: str):

    SILVER_TABLE = f"{SILVER_PATH}/acessos"

    df_acessos = (spark.read
            .format("delta")
            .load(str(SILVER_TABLE))
        )
      
    # dim tempo
    df_dim_tempo = (df_acessos
                        .select("ano", "mes")
                        .where(F.col("ano").isNotNull() & F.col("mes").isNotNull())
                        .dropDuplicates()
                        .withColumn("trimestre", F.expr("ceil(mes/3)"))
                        .withColumn("semestre", F.expr("ceil(mes/6)"))
                        .withColumn("sk_tempo", F.concat(F.col("ano"), F.lpad(F.col("mes"), 2, "0")).cast("int"))
                    )
    
    DIM_TEMPO_TABLE = f"{GOLD_PATH}/dim_tempo"

    (df_dim_tempo.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(DIM_TEMPO_TABLE))
    

    # dim geo
    df_dim_geo = (df_acessos
                        .select("codigo_ibge", "municipio", "uf", "regiao")
                        .where(F.col("uf").isNotNull())
                        .dropDuplicates()
                    )
       
    DIM_REGIAO_TABLE = f"{GOLD_PATH}/dim_geo"
    (df_dim_geo.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(DIM_REGIAO_TABLE))
    
    # dim empresa
    df_dim_empresa = (df_acessos
                        .select("cnpj", "empresa", "grupo_economico", "porte_prestadora")
                        .where(F.col("cnpj").isNotNull())
                        .dropDuplicates()
                    )
    
    DIM_EMPRESA_TABLE = f"{GOLD_PATH}/dim_empresa"
    (df_dim_empresa.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(DIM_EMPRESA_TABLE))
    
    # dim tecnologia
    df_dim_tecnologia = (df_acessos
                        .select("tecnologia", "meio_de_acesso", "faixa_velocidade")
                        .where(F.col("tecnologia").isNotNull())
                        .dropDuplicates()
                        .withColumn("sk_tecnologia", F.abs(F.hash("tecnologia","meio_de_acesso","faixa_velocidade")))
                    )
    
    DIM_TECNOLOGIA_TABLE = f"{GOLD_PATH}/dim_tecnologia"
    (df_dim_tecnologia.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(DIM_TECNOLOGIA_TABLE))
    
def create_fact(spark, SILVER_PATH: str, GOLD_PATH: str):

    SILVER_TABLE = f"{SILVER_PATH}/acessos"

    df = (spark.read
        .format("delta")
        .load(str(SILVER_TABLE))
    )


    # dim tempo
    df_dim_tempo = (spark.read
                    .format("delta")
                    .load(f"{GOLD_PATH}/dim_tempo")
    )

    # dim geo
    df_dim_geo = (spark.read
                    .format("delta")
                    .load(f"{GOLD_PATH}/dim_geo")
    )

    # dim empresa
    df_dim_empresa = (spark.read
                    .format("delta")
                    .load(f"{GOLD_PATH}/dim_empresa")
    )

    # dim tecnologia
    df_dim_tecnologia = (spark.read
                    .format("delta")
                    .load(f"{GOLD_PATH}/dim_tecnologia")
    )

    # fato acessos
    df_fato_acessos = (df
                        .groupBy(
                            "ano",
                            "mes",
                            "codigo_ibge",
                            "uf",
                            "regiao",
                            "cnpj",
                            "empresa",
                            "grupo_economico",
                            "porte_prestadora",
                            "tecnologia",
                            "meio_de_acesso",
                            "faixa_velocidade",
                            "tipo_pessoa",
                            "tipo_produto"
                        )
                        .agg(
                            F.sum("acessos").alias("total_acessos"),
                            F.round(F.avg("velocidade"), 2).alias("velocidade_media"), 
                            F.round(F.min("velocidade"), 2).alias("velocidade_minima"),
                            F.round(F.max("velocidade"), 2).alias("velocidade_maxima")
                        )
                    )

    df_fato_acessos = (df_fato_acessos
          .withColumn("sk_tempo", F.concat(F.col("ano"), F.lpad(F.col("mes"), 2, "0")).cast("int"))
          .withColumn("sk_tecnologia", F.abs(F.hash("tecnologia","meio_de_acesso","faixa_velocidade")))
    )

    # junção fato com dimensões    
    df_fato_acessos_final = (df_fato_acessos.alias("fato")
                        .join(df_dim_tempo, on="sk_tempo", how="inner")
                        .join(df_dim_geo, on="codigo_ibge", how="inner")
                        .join(df_dim_empresa, on="cnpj", how="inner")
                        .join(df_dim_tecnologia, on="sk_tecnologia", how="inner")
                        .select(
                            "sk_tempo",
                            "codigo_ibge",
                            "cnpj",
                            "sk_tecnologia",
                            "tipo_pessoa",
                            "tipo_produto",
                            "total_acessos",
                            "velocidade_media",
                            "velocidade_minima",
                            "velocidade_maxima"
                        )
                    )
    
    FATO_ACESSOS_TABLE = f"{GOLD_PATH}/fato_acessos"
    (df_fato_acessos_final.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(FATO_ACESSOS_TABLE))