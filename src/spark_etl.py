import argparse
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, mean, lower, upper, when
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
import os

DATA_DIR = "/opt/airflow/data"

def get_spark_session(app_name="ecommerce_etl"):
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def extract_to_bronze(spark):
    print("Extracting to Bronze...")
    tables = [
        "customers_dataset",
        "products_dataset",
        "orders_dataset",
        "order_items_dataset",
        "order_payments_dataset",
        "order_reviews_dataset",
        "sellers_dataset",
        "geolocation_dataset",
        "product_category_name_translation",
    ]
    
    for table in tables:
        df = spark.read.csv(
            f"{DATA_DIR}/raw/{table}.csv", header=True, inferSchema=True
        )
        df.write.mode("overwrite").parquet(f"{DATA_DIR}/bronze/{table}.parquet")
    print("Bronze extraction complete.")

def clean_to_silver(spark):
    print("Cleaning to Silver...")
    
    geo_agg = spark.read.parquet(
        f"{DATA_DIR}/bronze/geolocation_dataset.parquet"
    ).groupBy("geolocation_zip_code_prefix").agg(
        mean("geolocation_lat").alias("lat"),
        mean("geolocation_lng").alias("lng"),
    )
    
    order_timestamps = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
    ]
    timestamp_columns = order_timestamps + ["order_estimated_delivery_date"]
    orders = spark.read.parquet(
        f"{DATA_DIR}/bronze/orders_dataset.parquet"
    ).dropDuplicates(["order_id"])
    orders = orders.select(*[
        to_timestamp(col(name)).alias(name) if name in timestamp_columns else col(name)
        for name in orders.columns
    ])

    valid_timestamps = (
        col("order_status").isNull()
        | (col("order_status") != "delivered")
        | col("order_delivered_customer_date").isNotNull()
    )
    for index, earlier in enumerate(order_timestamps):
        for later in order_timestamps[index + 1:]:
            valid_timestamps = valid_timestamps & (
                col(earlier).isNull()
                | col(later).isNull()
                | (col(earlier) <= col(later))
            )
    orders = orders.filter(valid_timestamps)
    
    customers = spark.read.parquet(
        f"{DATA_DIR}/bronze/customers_dataset.parquet"
    ).dropna(subset=["customer_id"]).dropDuplicates(["customer_id"])
    customers = customers.withColumn(
        "customer_city", lower(col("customer_city"))
    ).withColumn(
        "customer_state", upper(col("customer_state"))
    ).fillna({"customer_city": "unknown", "customer_state": "NA"})
    customers = customers.join(
        geo_agg.select(
            col("geolocation_zip_code_prefix").alias("customer_zip_code_prefix"),
            col("lat").alias("customer_lat"),
            col("lng").alias("customer_lng"),
        ),
        on="customer_zip_code_prefix",
        how="left",
    )
    customers.write.mode("overwrite").parquet(
        f"{DATA_DIR}/silver/dim_customers.parquet"
    )
    
    sellers = spark.read.parquet(
        f"{DATA_DIR}/bronze/sellers_dataset.parquet"
    ).dropna(subset=["seller_id"]).dropDuplicates(["seller_id"])
    sellers = sellers.withColumn(
        "seller_city", lower(col("seller_city"))
    ).withColumn(
        "seller_state", upper(col("seller_state"))
    ).fillna({"seller_city": "unknown", "seller_state": "NA"})
    sellers = sellers.join(
        geo_agg.select(
            col("geolocation_zip_code_prefix").alias("seller_zip_code_prefix"),
            col("lat").alias("seller_lat"),
            col("lng").alias("seller_lng"),
        ),
        on="seller_zip_code_prefix",
        how="left",
    )
    sellers.write.mode("overwrite").parquet(
        f"{DATA_DIR}/silver/dim_sellers.parquet"
    )
    
    products = spark.read.parquet(
        f"{DATA_DIR}/bronze/products_dataset.parquet"
    ).dropDuplicates(["product_id"])
    products = products.withColumnRenamed(
        "product_name_lenght", "product_name_length"
    ).withColumnRenamed(
        "product_description_lenght", "product_description_length"
    )
    category_translation = spark.read.parquet(
        f"{DATA_DIR}/bronze/product_category_name_translation.parquet"
    )
    products = products.join(
        category_translation, on="product_category_name", how="left"
    )
    products.write.mode("overwrite").parquet(
        f"{DATA_DIR}/silver/dim_products.parquet"
    )
    
    order_items = spark.read.parquet(
        f"{DATA_DIR}/bronze/order_items_dataset.parquet"
    ).withColumn(
        "shipping_limit_date", to_timestamp(col("shipping_limit_date"))
    ).join(
        orders, on="order_id", how="inner"
    ).dropDuplicates(["order_id", "order_item_id"])
    valid_item = when(
        (col("price") > 0) & (col("freight_value") >= 0), True
    ).otherwise(False)
    valid_fct_items = order_items.filter(valid_item)
    invalid_fct_items = order_items.filter(~valid_item)
    valid_fct_items.write.mode("overwrite").parquet(
        f"{DATA_DIR}/silver/fct_order_items.parquet"
    )
    invalid_fct_items.write.mode("append").parquet(
        f"{DATA_DIR}/dlq/dlq_fct_order_items.parquet"
    )
    
    order_payments = spark.read.parquet(
        f"{DATA_DIR}/bronze/order_payments_dataset.parquet"
    ).join(
        orders, on="order_id", how="inner"
    ).dropDuplicates(["order_id", "payment_sequential"])
    order_payments.write.mode("overwrite").parquet(
        f"{DATA_DIR}/silver/fct_order_payments.parquet"
    )
    
    order_reviews = spark.read.parquet(
        f"{DATA_DIR}/bronze/order_reviews_dataset.parquet"
    ).join(
        orders, on="order_id", how="inner"
    ).withColumn(
        "review_creation_date", to_timestamp(col("review_creation_date"))
    ).withColumn(
        "review_answer_timestamp", to_timestamp(col("review_answer_timestamp"))
    ).dropDuplicates(["review_id", "order_id"])
    order_reviews.write.mode("overwrite").parquet(
        f"{DATA_DIR}/silver/fct_order_reviews.parquet"
    )
    
    print("Silver transformation complete.")

def load_to_gold(spark):
    print("Loading to Gold...")
    
    db_url = "jdbc:postgresql://datamart:5432/ecommerce"
    db_properties = {
        "user": "admin",
        "password": "password",
        "driver": "org.postgresql.Driver",
    }
    
    tables_pk = {
        "dim_customers": ("gold.dim_customers", ["customer_id"]),
        "dim_products": ("gold.dim_products", ["product_id"]),
        "dim_sellers": ("gold.dim_sellers", ["seller_id"]),
        "fct_order_items": ("gold.fct_order_items", ["order_id", "order_item_id"]),
        "fct_order_payments": ("gold.fct_order_payments", ["order_id", "payment_sequential"]),
        "fct_order_reviews": ("gold.fct_order_reviews", ["review_id", "order_id"]),
    }
    
    conn = psycopg2.connect(
        host="datamart",
        port=5432,
        dbname="ecommerce",
        user=db_properties["user"],
        password=db_properties["password"],
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    for file_name, (table_name, primary_keys) in tables_pk.items():
        df = spark.read.parquet(f"{DATA_DIR}/silver/{file_name}.parquet")
        temp_table = f"gold.temp_{file_name}"
        df.write.jdbc(
            url=db_url, table=temp_table, mode="overwrite", properties=db_properties
        )

        columns = ", ".join(f'"{column}"' for column in df.columns)
        conflict_columns = ", ".join(f'"{key}"' for key in primary_keys)
        updates = ", ".join(
            f'"{column}" = EXCLUDED."{column}"'
            for column in df.columns
            if column not in primary_keys
        )
        cursor.execute(
            f"INSERT INTO {table_name} ({columns}) "
            f"SELECT {columns} FROM {temp_table} "
            f"ON CONFLICT ({conflict_columns}) DO UPDATE SET {updates}"
        )
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
    
    cursor.close()
    conn.close()
    print("Gold load complete.")

def check_quality(spark):
    print("Running Data Quality Checks using Great Expectations...")
    # Import Great Expectations natively
    
    passed = True
    
    customers = spark.read.parquet(f"{DATA_DIR}/silver/dim_customers.parquet")
    customers_dataset = SparkDFDataset(customers)
    customer_checks = [
        ("row count > 0", customers_dataset.expect_table_row_count_to_be_between(min_value=1)),
        ("customer_id not null", customers_dataset.expect_column_values_to_not_be_null("customer_id")),
        ("customer_id unique", customers_dataset.expect_column_values_to_be_unique("customer_id")),
    ]
    for check_name, result in customer_checks:
        if not result["success"]:
            print(f"dim_customers quality check failed: {check_name}")
            passed = False
    
    order_items = spark.read.parquet(f"{DATA_DIR}/silver/fct_order_items.parquet")
    order_items_dataset = SparkDFDataset(order_items)
    order_item_checks = [
        ("row count > 0", order_items_dataset.expect_table_row_count_to_be_between(min_value=1)),
    ]
    if "order_status" in order_items.columns:
        order_item_checks.append(
            ("order_status valid", order_items_dataset.expect_column_values_to_be_in_set(
                "order_status",
                ["delivered", "shipped", "invoiced", "processing", "approved", "canceled", "unavailable", "created"],
            ))
        )
    for check_name, result in order_item_checks:
        if not result["success"]:
            print(f"fct_order_items quality check failed: {check_name}")
            passed = False
    
    dlq_order_items = spark.read.parquet(f"{DATA_DIR}/dlq/dlq_fct_order_items.parquet")
    dlq_count = dlq_order_items.count()
    total_count = order_items.count() + dlq_count
    if total_count > 0 and dlq_count / total_count >= 0.10:
        print(f"Warning: fct_order_items DLQ rate is {dlq_count / total_count:.2%} (>= 10%)")
        passed = False
    
    if not passed:
        raise Exception("Data Quality checks failed!")
    else:
        print("All Data Quality checks passed successfully via Great Expectations!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", choices=["bronze", "silver", "data_quality", "gold"], required=True)
    args = parser.parse_args()

    spark = get_spark_session()
    
    if args.layer == "bronze":
        extract_to_bronze(spark)
    elif args.layer == "silver":
        clean_to_silver(spark)
    elif args.layer == "data_quality":
        check_quality(spark)
    elif args.layer == "gold":
        load_to_gold(spark)
        
    spark.stop()
