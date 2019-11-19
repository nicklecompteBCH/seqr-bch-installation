from enum import Enum

from typing import (
    List, Optional
)

class FamilyMemberType(Enum):
    Index = 1
    Mother = 2
    Father = 3
    Sibling = 4
    Case = 5
    Control = 6

class SeqrSample:
    def __init__(
        self,
        individual_id: str,
        family_member_type: FamilyMemberType,
        path_to_vcf: str,
        path_to_bam : str
    ):
        self.individual_id = individual_id
        self.family_member_type = family_member_type
        self.path_to_vcf = path_to_vcf
        self.path_to_bam = path_to_bam



class SeqrFamily:

    def __init__(
        self,
        family_id : str,
        index_sample : SeqrSample,
        mother_sample : Optional[SeqrSample],
        father_sample : Optional[SeqrSample],
        other_samples : List[SeqrSample]
    ):
        if index_sample.family_member_type != FamilyMemberType.Index:
            raise ValueError(f"SeqrFamily index_sample with individual_id {self.index_sample.individual_id} was not labelled an index")
        if mother_sample:
            if mother_sample.family_member_type != FamilyMemberType.Mother:
                raise ValueError(f"SeqrFamily index_sample with individual_id {self.index_sample.individual_id} was not labelled an index")
        if father_sample:
            if father_sample.family_member_type != FamilyMemberType.Father:
                raise ValueError(f"SeqrFamily index_sample with individual_id {self.index_sample.individual_id} was not labelled an index")

        self.family_id = family_id
        self.index_sample = index_sample
        self.mother_sample = mother_sample
        self.father_sample = father_sample
        self.other_samples = other_samples
