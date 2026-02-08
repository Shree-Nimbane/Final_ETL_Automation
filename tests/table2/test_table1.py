from src.utility.general_utility import BaseClass

from src.data_validations import data_quantity
import logging
logger = logging.getLogger(__name__)
class Test_Fitst_class(BaseClass):

    def test_count_check(self,read_data):
        logger.info("Starting count check test")
        source,target=read_data
        data_quality_check = data_quantity.Data_Quantity(source,target)
        status=data_quality_check.conunt_val()
        logger.info(f"Count check status = {status}")
        # source.show()
        assert status=='PASS'




