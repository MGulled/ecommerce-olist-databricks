from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, md5, concat_ws, sha2

#configuration
SOURCE_PATH = "abfss://sourcedata@ecommercestg.dfs.core.windows.net/product_category"


@dp.materialized_view(
    name="ecommerce_olist.bronze.category",
    comment="Raw data processing for category",
    table_properties={
        "quality":"bronze",
        "layer":"bronze",
        "source_format":"csv",
        "delta.enableChangeDataFeed" : "true",
        "delta.autoOptimize.optimizeWrite" : "true",
        "delta.autoOptimize.autoCompact" : "true"
    }
)
def category_bronze():
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("mode", "PERMISSIVE").option("mergeSchema","true").option("ColumnNameOfCorruptRecord", "_corrupt_record").load(SOURCE_PATH)

    df = df.withColumn("filename",col("_metadata.file_path")).withColumn("ingest_datetime", current_timestamp())
    return df
    

