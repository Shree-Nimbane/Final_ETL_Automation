import pytest
import logging

from src.data_validations import data_quantity
from src.utility.general_utility import BaseClass
from src.data_validations import scd_2_imp

logger = logging.getLogger(__name__)



class Test_Emp_class(BaseClass):


    def test_count_check(self, read_data):
        logger.info("Starting count check test")

        source_df, target_df = read_data

        data_quality_check = data_quantity.Data_Quantity(
            source_df,
            target_df
        )

        status = data_quality_check.conunt_val()

        logger.info(f"Count check status = {status}")

        assert status == "PASS"

    def test_only_in_source(self, read_data, read_yml):
        logger.info("Starting only-in-source test")

        source_df, target_df = read_data

        key_columns = read_yml["validations"]["count_check"]["key_columns"]

        dq = data_quantity.Data_Quantity(
            source_df,
            target_df
        )

        status=dq.recodes_only_in_source(key_columns=key_columns)
        logger.info(f"Count check status = {status}")
        assert  status=="PASS"



    def test_only_in_target(self, read_data, read_yml):
        logger.info("Starting only-in-target test")

        source_df, target_df = read_data

        key_columns = read_yml["validations"]["count_check"]["key_columns"]

        dq = data_quantity.Data_Quantity(
            source_df,
            target_df
        )

        status=dq.recodes_only_in_target(key_columns=key_columns)
        logger.info(f"Count check status = {status}")
        assert  status=="PASS"


    def test_scd2_check(self,read_yml,read_data):
        logger.info("Starting SCD2 validation")
        primary_keys=read_yml["validations"]['uniqueness_check']['unique_columns']

        compare_columns = read_yml["validations"]['data_compare_check']['key_column']

        spark = self.spark
        source, target = read_data

        scd_obj = scd_2_imp.SCD_IMP(source)

        scd_obj.scd_2(
            spark=spark,
            TableName="shri_ram",
            primary_keys=primary_keys,
            compare_columns=compare_columns,
        )

        logger.info("Completed SCD2 validation")



    # def test_scd_under_imp(self):
    #     source_df,target_df=self.read_data
    #     print()
    #     self.scd_2(source_df=source_df).show()


    # def test_under_scd(self):
    #     source_df, target_df = self.read_data
    #     value=self.scd_2_dynamic(source_df,target_path,primary_keys,value_columns)



