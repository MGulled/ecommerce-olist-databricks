from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.table(
    name = "ecommerce_olist.silver.sellers",
    comment = "Cleaned sellers table",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)

def sellers_silver ():
    df = spark.read.table("ecommerce_olist.bronze.sellers")

    df_silver = (
        df.select(
            F.trim(F.col("seller_id")).alias("seller_id"),
            (F.col("seller_zip_code_prefix")).alias("seller_zip_code"),
            F.trim(F.initcap(F.col("seller_city"))).alias("seller_city"),
            F.trim(F.col("seller_state")).alias("seller_state")
        )
            .withColumn("silver_processed_timestamp", F.current_timestamp())

        )
    return df_silver


