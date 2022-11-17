from pyspark.sql import SparkSession

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
spark = SparkSession.builder.getOrCreate()
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF()
df.count()
df.printSchema()

#spark-submit --master spark://87f4e9f51d1c:7077 /opt/spark-data/test.py
#spark-submit --master spark-master /opt/spark-data/test.py