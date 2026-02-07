import pytest
import json
import os
import yaml
from src.utility.env_config import input_path
from src.data_validations.flatten import flatten
from pyspark.sql.types import *
import logging


logger = logging.getLogger(__name__)


@pytest.mark.usefixtures('attach_spark', 'test_path')
class BaseClass:

    @property
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

    def read_db(self, config):

        prefix = config['cred_lookup'].upper()

        def get_env(suffix):
            val = os.getenv(f"{prefix}_{suffix}")
            if val is None:
                raise ValueError(f"Missing environment variable: {prefix}_{suffix}")
            return val

        try:
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

            logger.info("Database read successful")
            return df

        except ValueError as e:
            logger.error(str(e), exc_info=True)
            pytest.fail(str(e))

        except Exception:
            logger.error("Database read failed", exc_info=True)
            pytest.fail("Database connection/read failed")

    @property
    def read_data(self):
        config_data = self.read_yml
        # spark = self.spark
        source_config = config_data['source']
        target_config = config_data['target']
        validations_config = config_data['validations']
        if source_config['type'] == 'database':
            source_df = self.read_db(config=source_config)
        else:
            # df = spark.read.json(source_config['path'],multiLine=True)
            source_df = self.read_file(config=source_config)
        if target_config['type'] == 'database':
            target_df = self.read_db(config=target_config)
        else:
            target_df = self.read_file(config=target_config)
        return source_df, target_df

    def read_file(self, config):
        file_path = input_path(config['path'])
        spark = self.spark
        file_type = config['type'].lower()
        df = None
        if file_type == 'csv':
            reader = spark.read \
                .option("sep", config['options']['delimiter']) \
                .option("header", config['options']['header'])
            if config['schema'].lower() == 'y':
                df = reader.schema(self.read_schema).csv(file_path)
            else:
                df = reader.option("inferSchema", True).csv(file_path)
        elif file_type == 'json':
            reader = spark.read.option("multiLine", config['options']['multiline'])
            if config['schema'].lower() == 'y':
                df = reader.schema(self.read_schema).json(file_path)
            else:
                df = reader.json(file_path)
            df = flatten(df)
        elif file_type == 'parquet':
            df = spark.read.parquet(file_path)
        elif file_type == 'avro':
            df = spark.read.format("avro").load(file_path)
        elif file_type == 'text':
            df = spark.read.text(file_path)

        return df

    @property
    def read_schema(self):
        schema_path = os.path.join(self.path, 'schema.json')
        with open(schema_path, 'r') as schema_file:
            schema = StructType.fromJson(json.load(schema_file))
        return schema
