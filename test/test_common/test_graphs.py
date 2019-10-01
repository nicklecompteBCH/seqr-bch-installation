from common.graphs import *
import unittest
import random

def get_sample_int_star() -> Star[int,int]:
    mystar : Star[int,int] = Star(
        1,
        set([2,3,4,5,6]),
        is_directed=True
    )
    return mystar

def generate_random_star(n) -> Star[int,int]:
    max = 100000
    vertex_set = set()
    center = random.randint(0,max)
    vertex_set.add(center)
    for i in range(1,n):
        vertex_set.add(random.randint(0,max))
    vertex_set.remove(center)
    return Star(center, vertex_set, is_directed=(bool(random.randint(0,max) % 2)))

def get_sample_undireced_str_star() -> Star[str,str]:
    return Star(
        "master_node",
        set(["node1","node2","thirdnode","node_4:deactivated"]),
        is_directed=False
    )



def simple_linear_tree() -> Tree[int]:
    """In fact the path [1,2,3,4].
    """
    return Tree(
        empty=False,
        nonempty_tree=(
            1,
            [Tree(
                nonempty_tree=(
                    2,
                    [Tree(
                        nonempty_tree=(
                            3,
                            [Tree(nonempty_tree=(4,[]))]
                        )
                    )]
            )
        )]
    ))

class TestGraphs(unittest.TestCase):

    def test_star(self):
        """Sanity checking with the get_sample_int_star.
        """
        mystar = get_sample_int_star()
        assert mystar.center == 1
        assert mystar.leaves == set([2,3,4,5,6])
        assert mystar.get_vertex_set() == set([1,2,3,4,5,6])
        assert mystar.get_adjacency_map() == {
            1 : set([2,3,4,5,6]),
            2 : set(),
            3 : set(),
            4 : set(),
            5 : set(),
            6 : set()
        }

    def test_star_iteration(self):
        visited_vertices = set()
        mystar = generate_random_star(1000)
        for vertex in mystar:
            visited_vertices.add(vertex)
        assert visited_vertices == mystar.get_vertex_set()

    def test_star_is_valid_graph(self):
        """Generates a graph from the adjacency map of the test_int_star.
        If it is not a valid graph, it will throw an exception upon construction.
        """
        graph = Graph(get_sample_int_star().get_adjacency_map())
        assert True

    def test_undirected_star(self):
        undirected_star = get_sample_undireced_str_star()
        # what the vertex set should be
        assert undirected_star.get_vertex_set() == set(["master_node","node1","node2","thirdnode","node_4:deactivated"])
        assert undirected_star.get_adjacency_map() == {
            "master_node": {"node1","node2","thirdnode","node_4:deactivated"},
            "node1" : {"master_node"},
            "node2" : {"master_node"},
            "thirdnode" : {"master_node"},
            "node_4:deactivated" : {"master_node"},
        }

    def test_empty_tree_is_empty(self):
        """Basic properties of the empty tree.
        """
        empty_tree = Tree(empty=True)
        assert empty_tree.get_vertex_set() == set()
        assert empty_tree.is_leaf == False
        assert empty_tree.root is None
        assert empty_tree.get_adjacency_map() == {}

    def test_simple_linear_tree(self):
        test_tree = simple_linear_tree()
        assert test_tree.root == 1
        assert test_tree.get_vertex_set() == set([1,2,3,4])
        assert test_tree.get_adjacency_map() == {
            1 : set([2]),
            2 : set([3]),
            3 : set([4]),
            4 : set()
        }

if __name__ == '__main__':
    unittest.main()