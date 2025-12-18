import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window
from pyspark.sql.functions import radians, sin, cos, asin, sqrt
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import avg


spark = SparkSession.builder \
    .appName("InfiniteAnalytics") \
    .getOrCreate()

#print("Spark Session created successfully.")

files = glob.glob("C://Data//*.csv")

df = spark.read \
    .format("csv") \
    .option("header", "false") \
    .option("inferschema", "true") \
    .load(files)

# df.show(5)
# df.printSchema()

df = df.withColumnRenamed("_c0", "location_index") \
    .withColumnRenamed("_c1", "device_id") \
    .withColumnRenamed("_c2", "latitude") \
    .withColumnRenamed("_c3", "longitude") \
    .withColumnRenamed("_c4", "date") \
    .withColumnRenamed("_c5", "network") \
    .withColumnRenamed("_c6", "timestamp") \
    .withColumnRenamed("_c7", "pincode") \
    .withColumnRenamed("_c8", "city")

# df.show(5)
# df.printSchema()

df_with_window = df.withColumn("time_window", window(col('timestamp'),'10 minutes'))

# df_with_window.show(5)
# df_with_window.printSchema()


# def haversine(lat1, lon1, lat2, lon2):
#     return 2 * 6371000 * asin(
#         sqrt(
#             sin((radians(lat2) - radians(lat1)) / 2) ** 2 +
#             cos(radians(lat1)) * cos(radians(lat2)) *
#             sin((radians(lon2) - radians(lon1)) / 2) ** 2
#         )
#     )


## We will check for only one sample window 

# sample_window = df_with_window.select("time_window").limit(1).collect()[0][0]
# #df_sample = df_with_window.filter(col("time_window.start") == sample_window.start)
# # df_sample.show(5)

# # print(df_sample.count())


# pairs = df_sample.alias("a").crossJoin(df_sample.alias("b")) \
#     .filter(col("a.device_id") != col("b.device_id"))

# # pairs.show(5)

# # print(pairs.count())

# pairs = pairs.withColumn(
#     "distance_m",
#     2 * 6371000 * asin(
#         sqrt(
#             sin((radians(col("b.latitude")) - radians(col("a.latitude"))) / 2) ** 2 +
#             cos(radians(col("a.latitude"))) * cos(radians(col("b.latitude"))) *
#             sin((radians(col("b.longitude")) - radians(col("a.longitude"))) / 2) ** 2
#         )
#     )
# )

# # pairs.select("a.device_id", "b.device_id", "distance_m").show(5)

# nearby = pairs.filter(col("distance_m") <= 200)

# nearby.show(5)
# print(nearby.count())

# neighbor_counts = nearby.groupBy(col("a.device_id")).agg(
#     countDistinct(col("b.device_id")).alias("neighbor_count")
# )

# neighbor_counts.show(10)


# avg_neighbors = neighbor_counts.agg(avg("neighbor_count").alias("avg_neighbors"))
# avg_neighbors.show()



pairs = df_with_window.alias("a").join(
    df_with_window.alias("b"),
    (col("a.time_window.start") == col("b.time_window.start")) &
    (col("a.time_window.end") == col("b.time_window.end")) &
    (col("a.device_id") != col("b.device_id"))
)

pairs = pairs.withColumn(
    "distance_m",
    2 * 6371000 * asin(
        sqrt(
            sin((radians(col("b.latitude")) - radians(col("a.latitude"))) / 2) ** 2 +
            cos(radians(col("a.latitude"))) * cos(radians(col("b.latitude"))) *
            sin((radians(col("b.longitude")) - radians(col("a.longitude"))) / 2) ** 2
        )
    )
)


nearby = pairs.filter(col("distance_m") <= 200)

neighbor_counts = nearby.groupBy(
    col("a.device_id"),
    col("a.time_window")
).agg(
    countDistinct(col("b.device_id")).alias("neighbor_count")
)

avg_neighbors = neighbor_counts.groupBy("time_window") \
    .agg(avg("neighbor_count").alias("avg_neighbors"))

avg_neighbors.show(truncate=False)
