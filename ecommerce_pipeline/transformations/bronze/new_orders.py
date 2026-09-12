import pyspark.sql.functions as F
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp



SOURCE_PATH = "abfss://sourcedata@ecommercestg.dfs.core.windows.net/new_orders"



@dp.table(
    name="ecommerce_olist.bronze.new_orders",
    comment="Streaming Ingestion for new orders with Autoloader",
    table_properties={
        "quality":"bronze",
        "layer":"bronze",
        "source_format":"json",
        "delta.enableChangeDataFeed" : "true",
        "delta.autoOptimize.optimizeWrite" : "true",
        "delta.autoOptimize.autoCompact" : "true"
    }
)
def new_orders_bronze():
    df = (spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.inferColumnTypes", "true")
         .option("cloudFiles.schemaEvolutionMode", "rescue")
         .option("cloudFiles.maxFilesPerTrigger",100)
         .load(SOURCE_PATH)
    )
    

    df = df.withColumn("filename", F.col("_metadata.file_path")).withColumn("ingest_datetime", current_timestamp())
    return df
    