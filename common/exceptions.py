from enum import Enum
from typing import TypeVar

T = TypeVar("T")


class InvalidTypeException[T](Exception):

    def __init__(
        self,
        invalid_type: type
    ):
        self.invalid_type = invalid_type
        self.message = ""

