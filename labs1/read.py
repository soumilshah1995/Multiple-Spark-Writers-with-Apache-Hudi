import sys
import os

os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk@11'

try:
    import os
    import sys
    import pyspark
    from pyspark.sql import SparkSession

    print("Imports loaded")
except Exception as e:
    print("error", e)

HUDI_VERSION = '0.14.0'
SPARK_VERSION = '3.4'

SUBMIT_ARGS = f"--packages org.apache.hadoop:hadoop-aws:3.3.2,org.apache.hudi:hudi-spark{SPARK_VERSION}-bundle_2.12:{HUDI_VERSION} pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ['PYSPARK_PYTHON'] = sys.executable

# Set Spark and Hoodie configurations
spark = SparkSession.builder \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .getOrCreate()

path = "file:////Users/soumilshah/IdeaProjects/SparkProject/tem/database=default/table_name=orders"

# Load data and create temporary view
spark.read.format("hudi") \
    .load(path) \
    .createOrReplaceTempView("hudi_snapshot")

spark.sql("SELECT customerID,state,productSKU FROM hudi_snapshot ").show(truncate=False)
