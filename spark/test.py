from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("HDFSConnectionTest")
    .getOrCreate()
)

conf = spark.sparkContext._jsc.hadoopConfiguration()

print("Spark:", spark.version)
print("fs.defaultFS:", conf.get("fs.defaultFS"))

ratings = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("hdfs://localhost:9000/user/bdsp/hw3/raw/ratings.csv")
)

movies = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("hdfs://localhost:9000/user/bdsp/hw3/raw/movies.csv")
)

print("Ratings schema:")
ratings.printSchema()

print("Movies schema:")
movies.printSchema()

print("Ratings count:", ratings.count())
print("Movies count:", movies.count())

spark.stop()