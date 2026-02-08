import pytest
import json
import os
import yaml
from src.utility.env_config import input_path
from src.data_validations.flatten import flatten
from pyspark.sql.types import *
from pyspark import StorageLevel
import logging


logger = logging.getLogger(__name__)

@pytest.mark.usefixtures('attach_spark')
class BaseClass:


    # -------------------------------------------------
    # YAML FIXTURE
    # -------------------------------------------------
    @pytest.fixture(scope='class')
    def read_yml(self):
        if hasattr(self, "_cached_config"):
            return self._cached_config

        xpath = self.path
        config_path = os.path.join(xpath, 'config.yml')

        try:
            logger.info(f"Reading config file: {config_path}")

            with open(config_path, "r") as ts:
                value = yaml.safe_load(ts)

            if not value:
                raise ValueError("config.yml is empty")

            self._cached_config = value
            return value

        except FileNotFoundError:
            logger.error("config.yml not found", exc_info=True)
            pytest.fail(f"Config file not found: {config_path}")

        except yaml.YAMLError:
            logger.error("Invalid YAML format", exc_info=True)
            pytest.fail("Invalid YAML in config.yml")

        except Exception:
            logger.error("Unexpected error while reading YAML", exc_info=True)
            pytest.fail("Failed while reading config.yml")

    # -------------------------------------------------
    # READ DATA FIXTURE (SOURCE + TARGET)
    # -------------------------------------------------
    @pytest.fixture(scope="class")
    def read_data(self, read_yml):

        try:
            logger.info("Starting to read source and target data")

            source_config = read_yml["source"]
            target_config = read_yml["target"]

            # ---------------- SOURCE ----------------
            if source_config["type"] == "database":
                source_df = self.read_db(config=source_config)
            else:
                source_df = self.read_file(config=source_config)

            # ---------------- TARGET ----------------
            if target_config["type"] == "database":
                target_df = self.read_db(config=target_config)
            else:
                target_df = self.read_file(config=target_config)

            # 🔥 CACHE BOTH
            # logger.info("Caching source and target DataFrames")
            #
            # source_df.cache()
            # target_df.cache()
            #
            # # materialize cache
            # # source_df.count()
            # # target_df.count()
            #
            logger.info("Successfully read & cached source and target data")

            return source_df, target_df


        except Exception as e:
            logger.error("Failed while reading source/target data", exc_info=True)
            pytest.fail(f"Source / Target data read failed: {str(e)}")

    # -------------------------------------------------
    # DATABASE READER
    # -------------------------------------------------
    def read_db(self, config):
        prefix = config['cred_lookup'].upper()

        # Improved helper to log values for easier debugging
        def get_env(suffix):
            env_name = f"{prefix}_{suffix}"
            val = os.getenv(env_name)
            if val is None:
                raise ValueError(f"Missing environment variable: {env_name}")
            logger.debug(f"Fetched {env_name}: {val}")  # This helps catch "READ_DATA" issues
            return val

        try:
            # ---------------------------------------------------------
            # 1. 🔥 CACHE CHECK
            # ---------------------------------------------------------
            # We use prefix + table name to ensure the cache key is unique
            cache_key = f"db:{prefix}:{get_env('TABLE')}"

            if not hasattr(self, "_df_cache"):
                self._df_cache = {}

            if cache_key in self._df_cache:
                logger.info("Reusing cached DB DataFrame for %s", cache_key)
                return self._df_cache[cache_key]

            # ---------------------------------------------------------
            # 2. DATABASE READ
            # ---------------------------------------------------------
            logger.info(f"Reading database using credential prefix: {prefix}")
            spark = self.spark

            df = spark.read.format('jdbc') \
                .option('url', get_env("URL")) \
                .option('user', get_env("USER")) \
                .option('password', get_env("PASSWORD")) \
                .option('database', get_env("DATABASE")) \
                .option('schema', get_env("SCHEMA")) \
                .option('dbtable', get_env("TABLE")) \
                .option('driver', get_env("DRIVER")) \
                .load()

            # ---------------------------------------------------------
            # 3. 🔥 PERSIST & STORE IN CACHE
            # ---------------------------------------------------------
            # We persist so Spark doesn't re-query the DB for every test
            df = df.persist(StorageLevel.MEMORY_AND_DISK)
            df.count()  # Forces the query to run NOW (catches errors early)

            self._df_cache[cache_key] = df
            logger.info("Database read & cached successfully")

            return df

        except ValueError as e:
            logger.error(str(e), exc_info=True)
            pytest.fail(str(e))
        except Exception as e:
            logger.error(f"Database read failed: {str(e)}", exc_info=True)
            pytest.fail(f"Database connection/read failed: {str(e)}")

    # -------------------------------------------------
    # FILE READER
    # -------------------------------------------------

    from pyspark import StorageLevel

    def read_file(self, config):

        try:
            file_path = input_path(config["path"])
            spark = self.spark
            file_type = config["type"].lower()

            # -------------------------------
            # 🔥 CREATE CACHE KEY
            # -------------------------------
            cache_key = f"{file_type}:{file_path}"

            # -------------------------------
            # 🔥 INIT CACHE DICT ONCE
            # -------------------------------
            if not hasattr(self, "_df_cache"):
                self._df_cache = {}

            # -------------------------------
            # 🔥 RETURN IF ALREADY LOADED
            # -------------------------------
            if cache_key in self._df_cache:
                # logger.info("Reusing cached DataFrame for %s", cache_key)
                return self._df_cache[cache_key]

            logger.info("Reading file type=%s path=%s", file_type, file_path)

            if file_type == "csv":

                reader = (
                    spark.read
                    .option("sep", config["options"]["delimiter"])
                    .option("header", config["options"]["header"])
                )

                if config["schema"].lower() == "y":
                    df = reader.schema(self.read_schema).csv(file_path)
                else:
                    df = reader.option("inferSchema", True).csv(file_path)

            elif file_type == "json":

                reader = spark.read.option(
                    "multiLine", config["options"]["multiline"]
                )

                if config["schema"].lower() == "y":
                    df = reader.schema(self.read_schema).json(file_path)
                else:
                    df = reader.json(file_path)

                df = flatten(df)

            elif file_type == "parquet":
                df = spark.read.parquet(file_path)

            elif file_type == "avro":
                df = spark.read.format("avro").load(file_path)

            elif file_type == "text":
                df = spark.read.text(file_path)

            else:
                raise ValueError(f"Unsupported file type: {file_type}")

            # -------------------------------
            # 🔥 CACHE + MATERIALIZE
            # -------------------------------
            df = df.persist(StorageLevel.MEMORY_AND_DISK)
            df.count()

            self._df_cache[cache_key] = df

            logger.info("File read & cached successfully")

            return df

        except Exception:
            logger.error("File read failed", exc_info=True)
            pytest.fail("File read failed")

    # -------------------------------------------------
    # SCHEMA
    # -------------------------------------------------
    @property
    def read_schema(self):
        schema_path = os.path.join(self.path, 'schema.json')

        with open(schema_path, 'r') as schema_file:
            schema = StructType.fromJson(json.load(schema_file))

        return schema
