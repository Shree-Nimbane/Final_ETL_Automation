import pytest
import json
import os
import yaml
from src.utility.env_config import input_path
from src.data_validations.flatten import flatten
from delta.tables import *
from pyspark.sql.types import *
from delta import *
from pyspark.sql.functions import lit, concat
from pyspark.sql import functions as F

from pyspark.sql.functions import xxhash64
from operator import and_


@pytest.mark.usefixtures('spark_session')
class BaseClass:

    @property
    def read_yml(self):
        xpath = self.path
        # config_path = xpath + '\\config.yml'
        config_path = os.path.join(xpath, 'config.yml')
        with open(config_path, "r") as ts:
            value = yaml.safe_load(ts)
        return value

    def read_db(self, config):
        spark = self.spark
        cred_lookup = config['cred_lookup']
        # in real time is it already connected?
        df = spark.read.format('jdbc'). \
            option('url', os.getenv(f"{cred_lookup}_URL")). \
            option('user', os.getenv(f"{cred_lookup}_USER")). \
            option('password', os.getenv(f"{cred_lookup}_PASSWORD")). \
            option('database', os.getenv(f"{cred_lookup}_DATABASE")). \
            option('schema', os.getenv(f"{cred_lookup}_SCHEMA")). \
            option('dbtable', os.getenv(f"{cred_lookup}_TABLE")). \
            option('driver', os.getenv(f"{cred_lookup}_DRIVER")). \
            load()
        return df

    @property
    def read_data(self):
        config_data = self.read_yml
        spark = self.spark
        source_config = self.read_yml['source']
        target_config = self.read_yml['target']
        validations_config = self.read_yml['validations']
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

