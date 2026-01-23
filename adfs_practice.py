from delta import *
from delta.tables import *
from delta import configure_spark_with_delta_pip
from py4j.java_gateway import java_import
from pyspark.sql import SparkSession

snow_jar = 'C:\\Users\\shiva\\PySpark_code\\pycharmProject\\ETL_Auto_01\\ETL_Automation\\jars\\snowflake-jdbc-3.22.0.jar'
azure_jar = 'C:\\Users\\shiva\\PySpark_code\\pycharmProject\\ETL_Auto_01\\ETL_Automation\\jars\\azure-storage-8.6.6.jar'
hadoop_jar='C:\\Users\\shiva\\PySpark_code\\pycharmProject\\ETL_Auto_01\\ETL_Automation\\jars\\hadoop-azure-3.3.1.jar'
jar_path = snow_jar+','+azure_jar+','+hadoop_jar


spark = (SparkSession.builder.master("local[1]")
        .appName("pytest_framework")
        .config("spark.jars", jar_path)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraClassPath", jar_path)
        .config("spark.executor.extraClassPath", jar_path)
        )
spark=configure_spark_with_delta_pip(spark).getOrCreate()

spark.conf.set(
  "fs.azure.account.key.decauto21.dfs.core.windows.net",
  "9Gn7n0tlNXAi7cImLo2zM375TWtCeWYFhJa+vcEXz6/MAgJcNTPn9xzC44EiwfFOnYZkQ1IrEajO+AStFp8xDQ=="
)
# java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
# java_import(spark._jvm, "org.apache.hadoop.fs.Path")
#
# hadoop_conf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.FileSystem.get(hadoop_conf)


df = spark.read.format("delta").load("abfss://test@decauto21.dfs.core.windows.net/raw/shri")


df.show(truncate=False)
from pyspark.sql.types import *
from pyspark.sql.functions import *
# schema=StructType([StructField('id', IntegerType()), StructField('name', StringType()),StructField('city', StringType()),StructField('country', StringType()),StructField('contact', IntegerType())])
# data=[(1001,"Michael",'New_York','USA',123456789)]
# df=spark.createDataFrame(data,schema)
# df.show()

# df.createOrReplaceTempView('source_view')
# spark.sql('select * from source_view').show()

# spark.sql('''CREATE OR REPLACE TABLE dim_employee (
#   emp_id INT ,
#   name STRING ,
#   city STRING,
#   country STRING,
#   contact INT
#   )
# USING DELTA
# LOCATION "C:/Users/Dinesh/spark-warehouse/dim_employee"
# ''')
#
# spark.sql('''MERGE INTO dim_employee as target
# USING source_view as source
# on target.emp_id =source.id
# when MATCHED
# THEN UPDATE SET
# target.name=source.name,
# target.city=source.city,
# target.country=source.country,
# target.contact=source.contact
# WHEN NOT MATCHED THEN
# INSERT (emp_id,name,country,city,contact) values(id,name,country, city,contact)''')
#
# spark.sql("select * from dim_employee").show()
# dim_emp=DeltaTable.forName(spark,'dim_employee')
#
# # dim_emp.toDF().show()
# #       using pyspark method
# df.write.format('delta').mode('overwrite').saveAsTable('source_delta1')
# source_instance=DeltaTable.forPath(spark,"C:/Users/Dinesh/spark-warehouse/source_delta1")
# # source_instance=DeltaTable.forName(spark,'source_delta1')
#
# # source_instance.toDF().show()
#
#
# dim_emp.alias('target').merge(
#         source=df.alias('source'),condition="target.emp_id=source.id"
# ).whenMatchedUpdate(set={
#         'name':'source.name',
#         'city':'source.city',
#         'country':'source.country',
#         'contact':'source.contact'
# }).whenNotMatchedInsert(values={
#         'emp_id':'source.id',
#         'name':'source.name',
#         'city':'source.city',
#         'country':'source.country',
#         'contact':'source.contact'
# }).execute()
#
# dim_emp.toDF().show()