from pyspark.sql.functions import col, year

source_path = "/Volumes/landing-zone/gov-br/banda-larga"
destination_path = "bronze.`gov-br"
current_year = datetime.now().strftime("%Y")

df = spark.read.format("csv").option("header", "true").load(source_path)

# Filter the data for the specific year
year_to_ingest = 2025
filtered_df = df.filter(year(col("date_column")) == year_to_ingest)

# Write the filtered data to the destination path
filtered_df.write.format("parquet").mode("overwrite").save(destination_path)

# Display the ingested data
display(filtered_df)