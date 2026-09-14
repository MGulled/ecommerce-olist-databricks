from pyspark import  pipelines as dp
import pyspark.sql.functions as F

@dp.table(
    name = "ecommerce_olist.silver.order_items",
    comment = "Cleaned order items table",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)

def order_items_silver():
    df = spark.read.table("ecommerce_olist.bronze.order_items")

    df_silver = (
        df.select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.col("order_item_id").alias("order_item_id"),
            F.trim(F.col("product_id")).alias("product_id"),
            F.trim(F.col("seller_id")).alias("seller_id"),
            F.col("shipping_limit_date").alias("shipping_date"),
            F.col("price").alias("price"),
            F.col("freight_value").alias("freight_value"), 
            )
            .withColumn("silver_processed_timestamp", F.current_timestamp())
                 
    )
    return df_silver