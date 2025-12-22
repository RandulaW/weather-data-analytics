from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as _max, ceil, monotonically_increasing_id

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("WeeklyMaxTempHottestMonths") \
    .getOrCreate()

# Load preprocessed weather data
df = spark.read.csv(
    "hdfs://namenode:9000/user/weather/input/weather_preprocessed.csv",
    header=True,
    inferSchema=True
)

# Step 1: Identify hottest months (highest avg max temperature)
monthly_avg = df.groupBy("year", "month").agg(
    avg("temperature_2m_max").alias("avg_max_temp")
)

hottest_months = monthly_avg.orderBy(
    col("avg_max_temp").desc()
).limit(3)

# Step 2: Filter original data to hottest months
df_hottest = df.join(
    hottest_months.select("year", "month"),
    on=["year", "month"],
    how="inner"
)

# Step 3: Approximate week number (1–4)
df_with_week = df_hottest.withColumn(
    "week",
    ceil(monotonically_increasing_id() % 28 / 7 + 1)
)

# Step 4: Weekly maximum temperatures
weekly_max = df_with_week.groupBy(
    "year", "month", "week"
).agg(
    _max("temperature_2m_max").alias("weekly_max_temperature")
).orderBy("year", "month", "week")

weekly_max.show(truncate=False)

weekly_max.write.mode("overwrite").csv(
    "hdfs://namenode:9000/user/weather/output/weekly_max_temperature",
    header=True
)

spark.stop()
