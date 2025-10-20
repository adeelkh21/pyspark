#creating pyspark session

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Lab02").getOrCreate()
sc = spark.sparkContext

----------

#import findspark
#findspark.init()
from pyspark import SparkContext
from pyspark.sql import SparkSession
sc = SparkContext("local")
spark = SparkSession.builder.getOrCreate()

------------
#colab env

!apt-get install openjdk-11-jdk-headless -qq > /dev/null

#Use a stable Spark mirror link
!wget -q https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -O spark.tgz

#Extract the downloaded tar file
!tar xf spark.tgz

#Install findspark
!pip install -q findspark

import os

#Set environment paths
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.5.1-bin-hadoop3"

#Verify
print("Environment paths set successfully!")

import findspark
findspark.init()

from pyspark.sql import SparkSession

#Create Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Lab06_PySpark") \
    .getOrCreate()

print("Spark session created successfully!")

