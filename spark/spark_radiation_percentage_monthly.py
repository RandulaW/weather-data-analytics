from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("MonthlyRadiationPercentage") \
    .getOrCreate()

# Load preprocessed weather data
df = spark.read.csv(
    "hdfs://namenode:9000/user/weather/input/weather_preprocessed.csv",
    header=True,
    inferSchema=True
)

# Flag rows where shortwave radiation > 15 MJ/m²
df_flagged = df.withColumn(
    "above_15",
    (col("shortwave_radiation") > 15).cast("int")
)

# Calculate percentage per month
result = df_flagged.groupBy("month").agg(
    (_sum("above_15") / count("*") * 100).alias("percentage_above_15_MJ")
).orderBy("month")

result.show(truncate=False)

result.write.mode("overwrite").csv(
    "hdfs://namenode:9000/user/weather/output/radiation_percentage",
    header=True
)

spark.stop()
