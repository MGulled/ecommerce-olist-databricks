from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="ecommerce_olist.silver.category",
    comment="Cleaned product category names, translated from Portuguese to English",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def category_silver():
    df = spark.read.table("ecommerce_olist.bronze.category")

    df_silver = (
        df.select(
            F.trim(F.lower(F.col("product_category_name"))).alias("category_name_pt"),
            F.trim(F.lower(F.col("product_category_name_english"))).alias("category_name_en")
        )
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )
    return df_silver