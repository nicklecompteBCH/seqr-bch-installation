from typing import Union
from enum import Enum

class SexChromosome(Enum):
    X = 1
    Y = 2

    def __str__(self):
        if self == SexChromosome.X:
            return "X"
        elif self == SexChromosome.Y:
            return "Y"

ChromosomeNumber = Union[int,SexChromosome]

class LocusInterval:

    def __init__(self,
    chromosome1: ChromosomeNumber,
    start: int,
    end: int,
    chromosome2: ChromosomeNumber = None
):
        self.chromosome1 = chromosome1
        self.start = start
        self.end = end

    def to_hail_expr(self):
        if not self.chromosome2:
            return f"{self.chromosome}:{self.start}-{self.end}"