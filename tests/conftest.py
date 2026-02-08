import os
import pytest
import logging
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from delta import configure_spark_with_delta_pip
from src.utility.logger_config import setup_logger

setup_logger()
load_dotenv()

logger = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def spark_session(request):

    env = os.getenv("ENV", "local")
    logger.info(f"Running tests in ENV={env}")

    builder = SparkSession.builder.appName("pytest_framework")

    if env == "local":

        taf_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        jars = [
            os.path.join(taf_path, "jars", "snowflake-jdbc-3.22.0.jar"),
            os.path.join(taf_path, "jars", "azure-storage-8.6.6.jar"),
            os.path.join(taf_path, "jars", "hadoop-azure-3.3.1.jar"),
        ]

        jar_path = ",".join(jars)

        logger.info("Configuring Spark for local mode with JARs")

        builder = (
            builder.master("local[1]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.jars", jar_path)
            .config("spark.driver.extraClassPath", jar_path)
            .config("spark.executor.extraClassPath", jar_path)
            .config("spark.sql.warehouse.dir", os.path.abspath("spark-warehouse"))
        )

    try:
        logger.info("Starting Spark session")
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

    except Exception:
        logger.error("Spark session creation failed", exc_info=True)
        pytest.fail("Spark could not start")

    adls_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    adls_key = os.getenv("AZURE_STORAGE_KEY")

    if adls_account and adls_key:

        logger.info("Configuring ADLS credentials")

        spark.conf.set(
            f"fs.azure.account.auth.type.{adls_account}.dfs.core.windows.net",
            "SharedKey",
        )
        spark.conf.set(
            f"fs.azure.account.key.{adls_account}.dfs.core.windows.net",
            adls_key,
        )

    else:
        logger.warning("ADLS credentials not found in env variables")



    yield spark

    logger.info("Stopping Spark session")
    spark.stop()


@pytest.fixture(scope="class")
def attach_spark(request, spark_session):
    request.cls.spark = spark_session
    request.cls.path = request.node.fspath.dirname