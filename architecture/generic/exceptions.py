"""A grab-bag of Exceptions and Enums/record types describing them.

In some way this modules describes a lot of the rules of valid configurations :)
"""

from enum import Enum
from common.exceptions import InvalidTypeException

class InvalidStorageConfiguration(Enum) :
    RootDeviceSelectedAsExternal = 1,
    TooSmallForApplicaiton = 2

class InvalidStorageConfigurationException(Exception):

    def __init__(
        self,
        exception_type: InvalidStorageConfiguration
    ):
        self.exception_type = exception_type

class InvalidScalingGroupConfiguration(Enum):
    ImpossibleParameters = 1,
    TooExpensive = 2