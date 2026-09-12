from pyspark import  pipelines as dp
import pyspark.sql.functions as F

@dp.table(
    name = "ecommerce_olist.silver.products",
    comment = "Cleaned products table",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)

def products_silver():
    df = spark.read.table("ecommerce_olist.bronze.products")

    df_silver = (
        df.select(
            F.trim(F.col("product_id")).alias("product_id"),
            F.trim(F.col("product_category_name")).alias("product_category_name"),
            F.col("product_name_lenght").alias("product_name_length"),
            F.col("product_description_lenght").alias("product_description_length"),
            F.col("product_photos_qty"),
            F.col("product_weight_g"),
            F.col("product_length_cm"),
            F.col("product_height_cm"),
            F.col("product_width_cm")
            )
            .withColumn("silver_processed_timestamp", F.current_timestamp())            
    )
    return df_silver