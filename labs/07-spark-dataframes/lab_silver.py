"""Lab 07 - Bronze to Silver with Spark DataFrames and Spark SQL.

Use --mode local for repeatable validation with repository CSV files.
Use --mode s3 after loading the current Onyxia S3 environment variables.
"""
import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

parser = argparse.ArgumentParser()
parser.add_argument("--mode", choices=("local", "s3"), default="local")
args = parser.parse_args()

builder = SparkSession.builder.appName("lab-silver-pyspark").master("local[*]")
if args.mode == "s3":
    required = ["KUBERNETES_NAMESPACE", "AWS_S3_ENDPOINT", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"Missing Onyxia environment variables: {', '.join(missing)}")
    bucket = os.environ["KUBERNETES_NAMESPACE"]
    builder = (
        builder
        .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

if args.mode == "s3":
    users_path = f"s3a://{bucket}/bronze/users.csv"
    orders_path = f"s3a://{bucket}/bronze/orders.csv"
else:
    users_path = "users.csv"
    orders_path = "orders.csv"

users_bronze = spark.read.csv(users_path, header=True, inferSchema=True, samplingRatio=0.01, multiLine=True).cache()
orders_bronze = spark.read.csv(orders_path, header=True, inferSchema=True, samplingRatio=0.01, multiLine=True).cache()
print("bronze_counts", users_bronze.count(), orders_bronze.count())
users_bronze.select("address").show(3, truncate=False)

users_typed = (
    users_bronze.select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)

stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
).withColumn("city", F.when(F.col("city") == "", None).otherwise(F.col("city")))

orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)

duplicates = orders_typed.limit(3)
orders_with_duplicates = orders_typed.unionByName(duplicates)
dedup_window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())
stg_orders = (
    orders_with_duplicates
    .withColumn("row_number", F.row_number().over(dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

orders_typed.createOrReplaceTempView("orders_typed")
stg_orders_sql = spark.sql("""
    SELECT order_id, user_id, ordered_at, quantity, product
    FROM (
        SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) AS rn
        FROM orders_typed
    )
    WHERE rn = 1
""")
assert stg_orders.count() == orders_typed.count() == stg_orders_sql.count()
assert set(stg_orders.columns) == set(stg_orders_sql.columns)
assert stg_users.count() == users_bronze.count()
assert stg_orders.filter(F.col("quantity") <= 0).count() == 0
assert stg_orders.filter(~F.col("product").isin("bread", "brioche", "cookie", "croissant", "donut", "drink")).count() == 0
print("silver_counts", stg_users.count(), stg_orders.count())

# Exercise 1: validate birthdates against each user's first order.
first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_order_at"))
users_validated_df = (
    stg_users.join(first_orders, "user_id", "left")
    .withColumn("birthdate_is_valid", F.col("first_order_at").isNull() | (F.col("birthdate") <= F.to_date("first_order_at")))
)
stg_users.createOrReplaceTempView("stg_users")
stg_orders.createOrReplaceTempView("stg_orders")
users_validated_sql = spark.sql("""
    WITH first_orders AS (
        SELECT user_id, min(ordered_at) AS first_order_at
        FROM stg_orders GROUP BY user_id
    )
    SELECT u.*, f.first_order_at,
           f.first_order_at IS NULL OR u.birthdate <= CAST(f.first_order_at AS DATE) AS birthdate_is_valid
    FROM stg_users u LEFT JOIN first_orders f USING (user_id)
""")
assert users_validated_df.select("user_id", "birthdate_is_valid").exceptAll(
    users_validated_sql.select("user_id", "birthdate_is_valid")
).count() == 0
print("invalid_birthdates", users_validated_df.filter(~F.col("birthdate_is_valid")).count())

if args.mode == "s3":
    stg_users.write.mode("overwrite").parquet(f"s3a://{bucket}/silver/spark/users.parquet")
    stg_orders.write.mode("overwrite").parquet(f"s3a://{bucket}/silver/spark/orders.parquet")
else:
    output = Path("/tmp/ece-bigdata-lab07")
    stg_users.write.mode("overwrite").parquet(str(output / "users.parquet"))
    stg_orders.write.mode("overwrite").parquet(str(output / "orders.parquet"))
    assert spark.read.parquet(str(output / "users.parquet")).count() == stg_users.count()
    assert spark.read.parquet(str(output / "orders.parquet")).count() == stg_orders.count()
    print("local_parquet_output", output)

users_bronze.unpersist(); orders_bronze.unpersist(); spark.stop()
