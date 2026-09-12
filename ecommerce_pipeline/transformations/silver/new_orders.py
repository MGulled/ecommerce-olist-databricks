from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.view(
    name="new_orders_silver_staging",
    comment="Transformed new_orders data ready for CDC upsert"
)
@dp.expect("valid_order_id", "order_id IS NOT NULL")
@dp.expect("valid_order_item_id", "order_item_id IS NOT NULL")
@dp.expect("valid_price", "price >= 0")
@dp.expect("valid_freight", "freight_value >= 0")
@dp.expect("valid_order_status", 
    "order_status IN ('created', 'approved', 'processing', 'shipped', 'delivered', 'canceled', 'invoiced', 'unavailable')")
def new_orders_silver_staging():
    df_bronze = spark.readStream.table("ecommerce_olist.bronze.new_orders")

    df_silver = (
        df_bronze.select(
            F.regexp_replace(F.trim(F.col("order_id")), "-", "").alias("order_id"),
            F.col("order_item_id").cast("int").alias("order_item_id"),
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.lower(F.trim(F.col("order_status"))).alias("order_status"), 

            F.to_timestamp(F.col("order_purchase_timestamp")).alias("order_purchase_timestamp"),
            F.to_timestamp(F.col("order_approved_at")).alias("order_approved_at"),

            F.trim(F.col("product_id")).alias("product_id"),
            F.trim(F.col("seller_id")).alias("seller_id"),

            F.col("price").cast("decimal(10,2)").alias("price"),
            F.col("freight_value").cast("decimal(10,2)").alias("freight_value"),
        )
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )
    return df_silver


dp.create_streaming_table(
    name="ecommerce_olist.silver.new_orders",
    comment="Cleaned and validated new orders with CDC upsert capability",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)


dp.create_auto_cdc_flow(
    target="ecommerce_olist.silver.new_orders",
    source="new_orders_silver_staging",
    keys=["order_id", "order_item_id"],
    sequence_by=F.col("silver_processed_timestamp"),
    stored_as_scd_type=1,
    except_column_list=[],
)