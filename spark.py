import os
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Set environment variables
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-22"
os.environ["SPARK_HOME"] = "C:/Spark/spark-3.5.1-bin-hadoop3"
os.environ["HADOOP_HOME"] = "C:/Hadoop"
os.environ["PYSPARK_PYTHON"] = "C:/Users/chris/AppData/Local/Programs/Python/Python311/python.exe" 
os.environ["PATH"] = (
    os.path.join(os.environ["JAVA_HOME"], "bin") + os.pathsep +
    os.path.join(os.environ["SPARK_HOME"], "bin") + os.pathsep +
    os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep +
    os.path.join(os.environ["PYSPARK_PYTHON"]) + os.pathsep +
    os.environ["PATH"]
)

def create_session():
    conf = SparkConf().setAppName("SimpleTest").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return sc, spark

# Stop any existing Spark session or SparkContext
try:
    if 'sc' in globals() and sc is not None:
        sc.stop()
        print("--Stopped existing SparkContext")
    if 'spark' in globals() and isinstance(spark, SparkSession):
        spark.stop()
        print("--Stopped existing SparkSession")
except Exception as e:
    print(f"Error stopping existing Spark session or context: {e}")

# Create a new Spark session
sc, spark = create_session()
print("Spark session created successfully!")

# Create a simple RDD and perform a basic transformation
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)
print("Original RDD:", rdd.collect())

# Perform a simple transformation
squared_rdd = rdd.map(lambda x: x ** 2)
print("Squared RDD:", squared_rdd.collect())
