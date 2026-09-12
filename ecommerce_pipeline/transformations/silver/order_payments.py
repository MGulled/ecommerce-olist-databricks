from pyspark import  pipelines as dp
import pyspark.sql.functions as F

@dp.table(
    name = "ecommerce_olist.silver.order_payments",
    comment = "Cleaned order payments table",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)

def order_payments_silver():
    df = spark.read.table("ecommerce_olist.bronze.order_payments")

    df_silver = (
        df.select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.col("payment_sequential").alias("payment_sequential"),
            F.trim(F.col("payment_type")).alias("payment_type"),
            F.col("payment_installments").alias("payment_installments"),
            F.col("payment_value").alias("payment_value")
            )
            .withColumn("silver_processed_timestamp", F.current_timestamp())
    )             

    return df_silver