from enum import Enum
from common.graphs import Star, Tree

from machine import VirtualMachineInstance

class CommunicationType(Enum):
    TCP = 1,
    RFC = 2,
    SSH = 3,
    Local = 4

class WorkerNode:

    def __init__(
        self,
        node_instance : VirtualMachineInstance,
        master_instance: VirtualMachineInstance
    ):

        self.node_instance = node_instance
        self.master_instance = master_instance

class MasterNode:

    def __init__(self,
        local_topology: Star[VirtualMachineInstance, WorkerNode],
        communication_type: CommunicationType
    )

