from enum import Enum
from typing import List
from common.exceptions import InvalidTypeException

from exceptions import (
    InvalidStorageConfiguration,
    InvalidStorageConfigurationException,
)

class MachineSize:
    """Abstract representation of (for instance) an EC2 machine type.
    """

    def __init__(
        self,
        cpu_count : int,
        ram_GB : int
    ):
        self.cpu_count = cpu_count
        self.ram_GB = ram_GB

    def __eq__(self, other) :
        if other is not MachineSize:
            return False
        return (self.cpu_count == other.cpu_count) and (self.ram_GB == other.ram_GB)

    def __hash__(self) :
        return cpu_count * ram_GB

class MachineSizeCategory(Enum) :
    """Qualitative descriptions of a machine size.
    """
    Nano = 1,
    Micro = 2,
    Medium = 3,
    Large = 4,
    XLarge = 5,
    TwoXLarge = 6,
    FourXLarge = 7

class StorageType(Enum) :
    RootDevice = 1,
    MountedHDD = 2,
    MountedSDD = 3

class StorageDevice:
    def __init__(
        self,
        size_GB: int,
        storage_type: StorageType
    ):
        self.size_GB = size_GB
        self.storage_type = storage_type

    def __eq__(self, other):
        if other is not StorageDevice:
            return False
        return (self.size_GB == other.size_GB) and (self.storage_type == other.storage_type)

class RootDevice(StorageDevice):
    """Root devices are important enough to get their own class.
    """
    def __init__(
        self,
        size_GB: int
    ):
        super().__init__(size_GB, StorageType.RootDevice)

        def __eq__(self, other):
            return super().__eq__(self, other)

class VirtualMachine:
    """ A "complete" abstract description of a virtual machine.
    """

    def __init__(
        self,
        size: MachineSize,
        size_descriptiton: MachineSizeCategory,
        root_device: RootDevice,
        attached_devices : List[StorageDevice]
    ):
        # validate the list of attached devices :(
        if StorageType.RootDevice in set(map(lambda x: x.storage_type, attached_devices)):
            raise InvalidStorageConfigurationException(InvalidStorageConfiguration.RootDeviceSelectedAsExternal)
        self.size = size
        self.size_descriptiton = size_descriptiton
        self.root_device = root_device
        self.attached_devices = attached_devices

    def __eq__(self, other):
        if other is not VirtualMachine:
            raise InvalidTypeException(type(other))
        return (
            self.size == other.size and
            self.size_descriptiton == other.size_descriptiton and
            self.root_device == other.root_device and
            self.attached_devices == other.attached_devices
        )

class VirtualMachineInstance:

    def __init__(
        self,
        machine: VirtualMachine,
        id: str
    )