"""
Data nodes —
    stores data and executes data-related operations
    such as search and aggregation
Master nodes —
    in charge of cluster-wide management and configuration actions
    such as adding and removing nodes
Client nodes —
    forwards cluster requests to the master node and data-related
    requests to data nodes
Ingest nodes —
    for pre-processing documents before indexing
"""
from enum import Enum

class NodeRole(Enum):
    Data = 1
    Client = 2
    Ingest = 3
    Master = 4

class ElasticSearchNode:
    """
    :param id: Nodes are automatically assigned a unique identifier
    if none is provided.
    """
    def __init__(
        self,
        id: str = None,
        role: NodeRole
    )