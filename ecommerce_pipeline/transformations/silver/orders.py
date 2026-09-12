from pyspark import  pipelines as dp
import pyspark.sql.functions as F

@dp.table(
    name = "ecommerce_olist.silver.orders",
    comment = "Cleaned order table",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)

def orders_silver():
    df = spark.read.table("ecommerce_olist.bronze.orders")

    df_silver = (
        df.select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.trim(F.col("order_status")).alias("order_status"),
            F.col("order_purchase_timestamp").alias("purchase_date"),
            F.col("order_approved_at").alias("approved_date"),
            F.col("order_delivered_carrier_date").alias("carrier_delivery_date"),
            F.col("order_delivered_customer_date").alias("customer_delivery_date"),
            F.col("order_estimated_delivery_date").alias("estimated_delivery_date"),
            )
            .withColumn("silver_processed_timestamp", F.current_timestamp())
                 
    )
    return df_silver