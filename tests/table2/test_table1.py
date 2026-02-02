from src.utility.general_utility import BaseClass

from src.data_validations import data_quantity

class Test_Fitst_class(BaseClass):

    def test_count_check(self):
        source,target=self.read_data
        data_quality_check = data_quantity.Data_Quantity(*self.read_data)
        status=data_quality_check.conunt_val()
        # source.show()
        assert status=='PASS'




