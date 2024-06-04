try:
    import os
    import sys
    import uuid
    import pyspark
    import datetime
    from pyspark.sql import SparkSession
    from pyspark import SparkConf, SparkContext
    from faker import Faker
    import datetime
    from datetime import datetime
    import random
    import pandas as pd
    from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType
    from pyspark.sql.functions import col

    print("Imports loaded ")

except Exception as e:
    print("error", e)

HUDI_VERSION = '0.14.0'
SPARK_VERSION = '3.4'

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11"
SUBMIT_ARGS = f"--packages org.apache.hudi:hudi-spark{SPARK_VERSION}-bundle_2.12:{HUDI_VERSION} pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ['PYSPARK_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
    .config('className', 'org.apache.hudi') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .getOrCreate()

schema = StructType([
    StructField("orderID", StringType(), True),
    StructField("productSKU", StringType(), True),
    StructField("customerID", StringType(), True),
    StructField("orderDate", StringType(), True),
    StructField("orderAmount", FloatType(), True),
    StructField("state", StringType(), True)
])


def write_to_hudi(spark_df,
                  table_name,
                  db_name,
                  method='upsert',
                  table_type='COPY_ON_WRITE',
                  recordkey='',
                  precombine='',
                  partition_fields='',
                  curr_region='us-east-1',
                  index_type='BLOOM'
                  ):
    path = f"file:///Users/soumilshah/IdeaProjects/SparkProject/tem/database={db_name}/table_name={table_name}"

    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.table.type': table_type,
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': method,
        'hoodie.datasource.write.recordkey.field': recordkey,
        'hoodie.datasource.write.precombine.field': precombine,
        "hoodie.datasource.write.partitionpath.field": partition_fields,


        "hoodie.write.concurrency.mode": "optimistic_concurrency_control",
        "hoodie.cleaner.policy.failed.writes": "LAZY",
        "hoodie.write.lock.provider": "org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider",

    }
    #  optimistic_concurrency_control
    #  non_blocking_concurrency_control
    print(hudi_options)

    print("\n")
    print(path)
    print("\n")

    spark_df.write.format("hudi"). \
        options(**hudi_options). \
        mode("append"). \
        save(path)


# Initial data
data = [
    ("order002", "prod002", "cust002", "2024-01-16", 200.00, "NY"),
    ("order007", "prod007", "cust007", "2024-01-21", 400.00, "NY")
]

# Loop to update productSKU and write to Hudi
for i in range(1, 10):  # Number of iterations
    # Update productSKU
    updated_data = [(orderID, f"{productSKU[:-1]}update{i}", customerID, orderDate, orderAmount, state)
                    for (orderID, productSKU, customerID, orderDate, orderAmount, state) in data]

    # Create the DataFrame with updated data
    df = spark.createDataFrame(updated_data, schema)

    # Show the DataFrame with the updated "productSKU" column
    df.show()

    # Write to Hudi
    write_to_hudi(
        spark_df=df,
        method="upsert",
        db_name="default",
        table_name="orders",
        recordkey="orderID",
        precombine="orderDate",
        partition_fields="state"
    )
    import time

    time.sleep(2)

spark.stop()
