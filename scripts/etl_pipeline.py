from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, dense_rank
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.window import Window
import os

def create_spark_session():
    spark = SparkSession.builder \
        .appName("RetailSalesPipeline") \
        .master("local[*]") \
        .getOrCreate()
    return spark


def load_data(spark):
    required_files = {
        "transactions": "data/raw/transactions.csv",
        "products": "data/raw/products.csv",
        "stores": "data/raw/stores.csv",
    }

    for name, path in required_files.items():
        if not os.path.exists(path):
            raise FileNotFoundError(f"Required input file missing: '{path}' (expected for '{name}')")

    try:
        transactions = spark.read.csv(required_files["transactions"], header=True, inferSchema=True)
        products = spark.read.csv(required_files["products"], header=True, inferSchema=True)
        stores = spark.read.csv(required_files["stores"], header=True, inferSchema=True)
    except Exception as e:
        raise RuntimeError(f"Failed to load input data: {e}") from e

    return transactions, products, stores


def join_data(transactions, products, stores):
    required_columns = {
        "transactions": (transactions, ["product_id", "store_id"]),
        "products": (products, ["product_id"]),
        "stores": (stores, ["store_id"]),
    }

    for df_name, (df, cols) in required_columns.items():
        missing = [c for c in cols if c not in df.columns]
        if missing:
            raise ValueError(f"'{df_name}' DataFrame is missing required column(s): {missing}")

    joined_df = transactions \
        .join(broadcast(products), on="product_id", how="inner") \
        .join(broadcast(stores), on="store_id", how="inner")

    return joined_df


def get_top_product_per_city(joined_df):
    window_spec = Window.partitionBy("city").orderBy(col("total_quantity").desc())

    sales_by_product_city = joined_df \
        .groupBy("city", "product_name") \
        .agg(spark_sum("quantity").alias("total_quantity")) \
        .withColumn("ranked", dense_rank().over(window_spec))

    top_product_per_city = sales_by_product_city.filter(col("ranked") == 1)
    return top_product_per_city


def write_output(df, path):
    df.write.mode("overwrite").partitionBy("city").parquet(path)


def main():
    spark = create_spark_session()
    transactions, products, stores = load_data(spark)

    print(f"Transactions count: {transactions.count()}")
    print(f"Products count: {products.count()}")
    print(f"Stores count: {stores.count()}")

    joined_df = join_data(transactions, products, stores)
    top_products = get_top_product_per_city(joined_df)

    top_products.show()

    write_output(top_products, "data/output/top_products")
    print("\nPipeline finished. Output written to data/output/top_products")


if __name__ == "__main__":
    main()