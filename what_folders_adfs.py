from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from py4j.java_gateway import java_import

snow_jar = r'C:\Users\shiva\PySpark_code\pycharmProject\ETL_Auto_01\ETL_Automation\jars\snowflake-jdbc-3.22.0.jar'
azure_jar = r'C:\Users\shiva\PySpark_code\pycharmProject\ETL_Auto_01\ETL_Automation\jars\azure-storage-8.6.6.jar'
hadoop_jar = r'C:\Users\shiva\PySpark_code\pycharmProject\ETL_Auto_01\ETL_Automation\jars\hadoop-azure-3.3.1.jar'
jar_path = ','.join([snow_jar, azure_jar, hadoop_jar])

spark = (SparkSession.builder.master("local[1]")
         .appName("List_ADLS_Files")
         .config("spark.jars", jar_path)
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.driver.extraClassPath", jar_path)
         .config("spark.executor.extraClassPath", jar_path)
        )
spark = configure_spark_with_delta_pip(spark).getOrCreate()

# Hadoop config for ABFS
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem")
hadoop_conf.set("fs.azure.account.key.decauto21.dfs.core.windows.net",
                "9Gn7n0tlNXAi7cImLo2zM375TWtCeWYFhJa+vcEXz6/MAgJcNTPn9xzC44EiwfFOnYZkQ1IrEajO+AStFp8xDQ==")

java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
java_import(spark._jvm, "org.apache.hadoop.fs.Path")

# Define the base URI and the full path
base_uri = "abfss://test@decauto21.dfs.core.windows.net"
folder_path = "abfss://test@decauto21.dfs.core.windows.net/raw/"

# Create a Java URI object
java_uri = spark._jvm.java.net.URI(base_uri)

# Pass the URI AND the config to get the correct FileSystem instance
fs = spark._jvm.FileSystem.get(java_uri, hadoop_conf)

# Define the Path object
path = spark._jvm.Path(folder_path)

# Now list the status
file_status = fs.listStatus(path)

print("Files in folder:")
for file in file_status:
    print(file.getPath().getName())

# this under entire adls
'''Files in folder:
raw
test_folder'''


# this under raw folder
'''Files in folder:
Contact_info_t.csv
customer'''

