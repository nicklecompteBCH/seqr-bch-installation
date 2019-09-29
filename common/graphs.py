from typing import TypeVar, List, Dict, Generic, Union, Set, Tuple
from common.exceptions import InvalidTypeException

T = TypeVar("T")
U = TypeVar("U")

class Star(Generic[T,U]):

    def __init__(
        self,
        center : T,
        leaves: List[U]
    ):
        self.center = center
        self.leaves = leaves

TNode = TypeVar("TNode")
TTree = TypeVar("TTree")

class Tree(Generic[TNode,TTree]):
    """A functional tree, somewhat hacked into Python's type system.

    :param TNode: The type of the Node of the tree.
    :param TTree: The type of the subtrees. Really it's Tree itself but
    python isn't quite smart enough for that.
    :param empty: Is the tree the empty tree? Defaults to False.
    :type empty: Boolean, optional
    :param root: The root of the tree is a Node with a list of TTrees.
    :type root: Union[None,List[TNode,TTree]]
    :raises ValueError: A value error is raised if:
        - the tree is said to be empty but passed a non-None root
        - the tree is not empty but the root is None

    """

    def __init__(
        self,
        empty=False,
        nonempty_tree: Union[Tuple[T, List[TTree]], None] = None
    ):
        # Validate the constructor:
        # Initialize to an invalid but (semantically) empty tree.
        self.empty = False
        self.root = None
        self.leaves = []
        if empty:
            self.empty = True
            # Now the tree is valid. We need to make sure that the root
            # is either itself empty (which is sloppy but OK),
            # or None (better)
            if nonempty_tree is not None:
                raise ValueError("Tree was supplied True for empty in constructor, but was also supplied a nonempty tree")
        elif nonempty_tree is None:
            raise ValueError("Tree was said to be nonempty but was supplied empty root in its constructor")
        else:
            self.root = nonempty_tree[0]
            self.leaves = nonempty_tree[1]

def __validate_adjancency_map_of_graph(
    adjacency_map: Dict[T, List[T]],
    loops_allowed = False,
    is_directed = True,
    hypergraph_allowed = False
) -> bool:

    vertex_set = set(adjacency_map.keys())
    for vertex in vertex_set:
        adjacent_vertices = adjacency_map[vertex]
        for adj in adjacent_vertices:
            if adj not in vertex_set:
                raise ValueError(f"Vertex {adj} was not in the vertex set of the provided adjacency map. It must have at least an empty entry.")
            if not loops_allowed:
                if adj == vertex:
                    raise ValueError(f"Loops were not allowed but found in the provided adjacency map. Loop vertex: {adj}")
            if not is_directed:
                other_direction_adjacency = set(adjacency_map[adj])
                if vertex not in other_direction_adjacency:
                    raise ValueError(f"Adjacency map was said to be undirected but the edge ({vertex,adj}) did not have a matching reverse.")
        if not hypergraph_allowed:
            set_adjacency = set(adjacent_vertices)
            if len(set_adjacency) != len(adjacent_vertices):
                for adj in adjacent_vertices:




class Graph(Generic[T]):
    """A graph is

    :param T: The type of the graph's vertices.
    :param adjacency: Adjacency is described by a Dictionary[node, list of nodes].
    :raises ValueError: If the

    """

    def __init__(
        self,
        adjacency: Dict[T, List[T]],
        is_directed = True
    ):

        self.vertices = list(adjacency.keys())
        self.adjacency = adjacency