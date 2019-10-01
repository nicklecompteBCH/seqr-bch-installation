from enum import Enum
from common.graphs import Star, Tree, Graph

from machine import VirtualMachineInstance

class CommunicationType(Enum):
    TCP = 1,
    RFC = 2,
    SSH = 3,
    Local = 4

class WorkerNode:
    """A WorkerNode is just a VirtualMachineInstance linked to a master VirtualMachineInstance.
    """

    def __init__(
        self,
        node_instance : VirtualMachineInstance,
        master_instance: VirtualMachineInstance
    ):

        self.node_instance = node_instance
        self.master_instance = master_instance

class MasterNode:
    """A MasterNode is a Star[VirtualMachineInstance, WorkerNode],
    with some validation to make sure that the WorkerNodes' master_instances
    point to the center of the star.

    :raises ValueError: [description]
    :raises ValueError: [description]
    :raises ValueError: [description]
    """

    def __init__(
        self,
        local_topology: Star[VirtualMachineInstance, WorkerNode],
        communication_type: CommunicationType
    ):
        for leaf in local_topology.leaves:
            if leaf.master_instance != local_topology.center:
                raise ValueError("Invalid topology on MasterNode: leaves must have the master_node as the star's center")
        self.master_node = local_topology.center
        self.workers = local_topology.leaves
        self.local_topology = local_topology

        # If it's local communication, make sure it's only one machine instance.
        if self.communication_type == CommunicationType.Local:
            leaf_instance_set = set(map(lambda x: x.node_instance, local_topology.leaves))
            if len(leaf_instance_set) != 1:
                raise ValueError("If MasterNode is communicating with workers locally, they must all be the same instance")
            else:
                leaf_instance = leaf_instance_set.pop()
                if leaf_instance != self.master_node:
                    raise ValueError("Local MasterNode cannot have distinct instances for its leaves")
        self.communication_type = communication_type

    def __contains__(self, item):
        if isinstance(item, WorkerNode):
            return item in self.workers
        elif isinstance(item, VirtualMachineInstance):
            return item == self.master_node or item in self.workers
        else:
            raise NotImplementedError(f"Type {type(item)} not implemented.")

    def __iter__(self):
        yield from self.local_topology

class ClusterUnit:
    """A ClusterUnit is a tree of clusters.
    """
    def __init__(
        self,
        local_topology: Tree[Cluster]
    ):
        self.local_topology = local_topology
        self.highest_master_node = local_topology.root
        self.subclusters = local_topology.branches
        self.nodes_in_cluster = local_topology.get_vertex_set()
        self.is_empty = local_topology.empty
        self.is_simple = local_topology.is_leaf

    def __contains__(self, item):
        if isinstance(item, Cluster):
            return item in self.local_topology
        elif isinstance(item, VirtualMachineInstance):

    def __iter__(self):
        for cluster in self.local_topology:
            yield from cluster

class Cluster:

    def __init__(
        self,
        local_topology: Graph[ClusterUnit]
    )