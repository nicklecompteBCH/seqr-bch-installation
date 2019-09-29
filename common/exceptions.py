from enum import Enum
from typing import TypeVar, Generic

T = TypeVar("T")


class InvalidTypeException(Generic[T],Exception):

    def __init__(
        self,
        invalid_type: type
    ):
        self.invalid_type = invalid_type
        self.message = ""

