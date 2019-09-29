from typing import TypeVar, List, Dict, Generic

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
