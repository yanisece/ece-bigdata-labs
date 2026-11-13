"""Lab 06 - Spark RDD API, including the four exercises."""
from operator import add
from time import perf_counter
import re

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")
print("partitions", users_lines.getNumPartitions(), orders_lines.getNumPartitions())
print("repartitioned_orders", orders_lines.repartition(8).getNumPartitions())

header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip())
print("orders_lineage", orders_data.toDebugString().decode())
print("orders_count", orders_data.count())

UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
print("users_physical_lines", users_lines.count())
print("users_records", users_records.count())

def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }

orders_rdd = orders_data.map(parse_order).cache()
print("orders_sample", orders_rdd.take(3))

user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
user_quantity_grouped = (
    orders_rdd.map(lambda o: (o["user_id"], o["quantity"]))
    .groupByKey()
    .mapValues(sum)
)
assert dict(user_quantity.collect()) == dict(user_quantity_grouped.collect())
print("quantity_by_user_sample", user_quantity.take(5))

def most_recent(a, b):
    return a if a["date"] > b["date"] else b

orders_dedup = orders_rdd.map(lambda o: (o["order_id"], o)).reduceByKey(most_recent).values()
print("dedup_counts", orders_rdd.count(), orders_dedup.count())

def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]

users_kv = users_records.map(parse_user_for_join).cache()
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))
enriched = orders_kv.join(users_kv)
print("joined_sample", enriched.take(3))

# Exercise 1: one-pass count and total, then average quantity per user.
zero = (0, 0)
def seq(acc, quantity):
    return acc[0] + 1, acc[1] + quantity
def comb(a, b):
    return a[0] + b[0], a[1] + b[1]
order_stats = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).aggregateByKey(zero, seq, comb)
averages = order_stats.mapValues(lambda value: value[1] / value[0])
print("average_quantity_sample", averages.take(5))

# Exercise 2: inject an unknown user and detect it with a left outer join.
fake = dict(orders_rdd.first())
fake["order_id"] = "00000000-0000-0000-0000-000000000001"
fake["user_id"] = "00000000-0000-0000-0000-000000000000"
orders_with_fake = orders_rdd.union(sc.parallelize([fake]))
missing_users = (
    orders_with_fake.map(lambda o: (o["user_id"], o))
    .leftOuterJoin(users_kv)
    .filter(lambda item: item[1][1] is None)
)
assert missing_users.count() == 1
print("missing_user_orders", missing_users.count())

# Exercise 3: the equivalent aggregation with the DataFrame API.
orders_df = spark.createDataFrame(orders_rdd)
quantity_df = orders_df.groupBy("user_id").sum("quantity")
print("dataframe_groups", quantity_df.count())
quantity_df.explain()
print("rdd_lineage", user_quantity.toDebugString().decode())

# Exercise 4: compare repeated actions with and without a cached parser.
t0 = perf_counter(); orders_rdd.count(); cached_orders_time = perf_counter() - t0
t0 = perf_counter(); user_quantity.count(); cached_aggregation_time = perf_counter() - t0
uncached = orders_data.map(parse_order)
t0 = perf_counter(); uncached.count(); uncached_orders_time = perf_counter() - t0
t0 = perf_counter(); uncached.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add).count(); uncached_aggregation_time = perf_counter() - t0
print("timings_seconds", {
    "cached_orders": round(cached_orders_time, 4),
    "cached_aggregation": round(cached_aggregation_time, 4),
    "uncached_orders": round(uncached_orders_time, 4),
    "uncached_aggregation": round(uncached_aggregation_time, 4),
})

orders_rdd.unpersist()
users_kv.unpersist()
spark.stop()
