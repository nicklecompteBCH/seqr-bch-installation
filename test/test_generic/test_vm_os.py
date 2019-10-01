from architecture.generic.vm_os import *
import unittest

class TestVMOS(unittest.TestCase):

    def test_export_works(self):
        # variable_name:str, variable_value: str, os_ttype
        test_command = export_variable("AWS_PROFILE","TESTPROFILE",os_type=OperatingSystem.CentOS)
        assert (str(test_command) == "export AWS_PROFILE=TESTPROFILE ")