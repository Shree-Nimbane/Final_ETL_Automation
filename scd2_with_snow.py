from src.utility.Base_Class import BaseClass
import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
import pytest
# BaseClass()

url='jdbc:snowflake://JIOHBRN-VO25590.snowflakecomputing.com'
user='SHREE'
password='ShivajiShree@99'
database='SNOWFLAKE_PATHS'
schema="SNOWFLAKE_PATHS.FILE_1"
dbtable='SNOWFLAKE_PATHS.FILE_1.FOR_TRY'

snow_jar = r'C:\Users\shiva\PySpark_code\pycharmProject\ETL_Auto_01\ETL_Automation\jars\snowflake-jdbc-3.22.0.jar'
postgres_jar = r'C:\Users\shiva\PySpark_code\pycharmProject\ETL_Auto_01\ETL_Automation\jars\postgresql-42.7.5.jar'
jar_path = snow_jar+','+postgres_jar


spark = (SparkSession.builder.master("local[1]")
        .appName("pytest_framework")
        .config("spark.jars", jar_path)
        .config("spark.sql.warehouse.dir","C:/Users/shiva/spark-warehouse")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraClassPath", jar_path)
        .config("spark.executor.extraClassPath", jar_path)
        )
spark=configure_spark_with_delta_pip(spark).getOrCreate()


df=spark.read.csv(r'C:\Users\shiva\PySpark_code\pycharmProject\ETL_Auto_01\ETL_Automation\input_files\Reading_all_data.csv',header=True)
df.show()