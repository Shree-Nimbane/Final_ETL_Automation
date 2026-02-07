import pytest
import logging

from src.data_validations import data_quantity
from src.utility.general_utility import BaseClass
from src.data_validations import scd_2_imp

logger = logging.getLogger(__name__)



class Test_Emp_class(BaseClass):
    # value = BaseClass.read_yml
    # data_read=BaseClass.read_data
    def test_count_check(self):
        logger.info("Starting count check test")
        data_quality_check = data_quantity.Data_Quantity(*self.read_data)
        status = data_quality_check.conunt_val()
        logger.info(f"Count check status = {status}")
        assert status == "PASS"

    def test_only_in_source(self):
        logger.info("Starting only-in-source test")
        key_columns = self.read_yml["validations"]["count_check"]["key_columns"]
        only_source = data_quantity.Data_Quantity(*self.read_data)
        only_source.recodes_only_in_source(key_columns=key_columns)
        logger.info("Completed only-in-source validation")

    def test_only_in_target(self):
        logger.info("Starting only-in-target test")

        key_columns = self.read_yml["validations"]["count_check"]["key_columns"]

        only_source = data_quantity.Data_Quantity(*self.read_data)
        only_source.recodes_only_in_target(key_columns=key_columns)

        logger.info("Completed only-in-target validation")

    def test_scd2_check(self):
        logger.info("Starting SCD2 validation")
        primary_keys=self.read_yml["validations"]['uniqueness_check']['unique_columns']
        compare_columns = self.read_yml["validations"]['data_compare_check']['key_column']

        spark = self.spark
        source, target = self.read_data

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



