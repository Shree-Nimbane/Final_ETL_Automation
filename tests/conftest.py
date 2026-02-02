import os
import pytest
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from delta import configure_spark_with_delta_pip

load_dotenv()

@pytest.fixture(scope="class")
def spark_session(request):

    env = os.getenv("ENV", "local")

    builder = SparkSession.builder.appName("pytest_framework")



    if env == "local":
        taf_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        jars = [
            os.path.join(taf_path, "jars", "snowflake-jdbc-3.22.0.jar"),
            os.path.join(taf_path, "jars", "azure-storage-8.6.6.jar"),
            os.path.join(taf_path,"jars", "hadoop-azure-3.3.1.jar"),
        ]

        jar_path = ",".join(jars)

        builder = (
            builder.master("local[1]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars", jar_path)
            .config("spark.driver.extraClassPath", jar_path)
            .config("spark.executor.extraClassPath", jar_path)
            .config("spark.sql.warehouse.dir", os.path.abspath("spark-warehouse"))
        )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    adls_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    adls_key = os.getenv("AZURE_STORAGE_KEY")

    if adls_account and adls_key:
        spark.conf.set(
            f"fs.azure.account.auth.type.{adls_account}.dfs.core.windows.net",
            "SharedKey",
        )
        spark.conf.set(
            f"fs.azure.account.key.{adls_account}.dfs.core.windows.net",
            adls_key,
        )
    path = request.node.fspath.dirname
    request.cls.path = path
    request.cls.spark = spark

    yield spark

    spark.stop()

# print(os.getenv('env','local'))