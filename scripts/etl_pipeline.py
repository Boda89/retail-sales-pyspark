from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, dense_rank
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.window import Window


def create_spark_session():
    spark = SparkSession.builder \
        .appName("RetailSalesPipeline") \
        .master("local[*]") \
        .getOrCreate()
    return spark


def load_data(spark):
    transactions = spark.read.csv("data/raw/transactions.csv", header=True, inferSchema=True)
    products = spark.read.csv("data/raw/products.csv", header=True, inferSchema=True)
    stores = spark.read.csv("data/raw/stores.csv", header=True, inferSchema=True)
    return transactions, products, stores


def join_data(transactions, products, stores):
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