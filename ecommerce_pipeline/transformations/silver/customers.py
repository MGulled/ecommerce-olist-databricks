from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.table(
    name = "ecommerce_olist.silver.customer",
    comment = "Cleaned customers table",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)


def customers_silver ():
    df = spark.read.table("ecommerce_olist.bronze.customer")

    df_silver = (
        df.select(
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.trim(F.col("customer_unique_id")).alias("unique_id"),
            F.trim(F.col("customer_zip_code_prefix")).alias("zip_code"),
            F.trim(F.initcap(F.col("customer_city"))).alias("city"),
            F.trim(F.col("customer_state")).alias("state")
        )
            .withColumn("silver_processed_timestamp", F.current_timestamp())

        )
    return df_silver


