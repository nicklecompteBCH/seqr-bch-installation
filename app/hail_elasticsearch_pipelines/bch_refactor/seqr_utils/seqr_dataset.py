from enum import Enum

from typing import (
    List, Optional, Set, Iterable
)

class BCHSeqrProject(Enum):
    AlanBeggs = 1
    ChrisWalsh = 2
    TimYu = 3
    Testing = 999

    @staticmethod
    def from_string(string):
        if string == "alan_beggs":
            return BCHSeqrProject.AlanBeggs

class FamilyMemberType(Enum):
    Index = 1
    Mother = 2
    Father = 3
    Sibling = 4
    Case = 5
    Control = 6

    @staticmethod
    def from_bchconnect_str(inputstr: str):
        inp = inputstr.lower()
        if inp == "index":
            return FamilyMemberType.Index
        elif inp == 'mother':
            return FamilyMemberType.Mother
        elif inp == 'father':
            return FamilyMemberType.Father
        elif inp in set(['sibling','twin_monozygotic','twin_dizygotic']):
            return FamilyMemberType.Sibling
        elif inp == 'case':
            return FamilyMemberType.Case
        elif inp == 'control':
            return FamilyMemberType.Control
        else:
            raise ValueError(f"Could not parse Seqr FamilyMemberType from input {inputstr}")

class SeqrSample:
    def __init__(
        self,
        individual_id: str,
        family_id : str,
        project: BCHSeqrProject,
        family_member_type: FamilyMemberType,
        path_to_vcf: str,
        path_to_bam : str
    ):
        self.individual_id = individual_id
        self.family_id = family_id
        self.project = project
        self.family_member_type = family_member_type
        self.path_to_vcf = path_to_vcf
        self.path_to_bam = path_to_bam

    def __hash__(self):
        return hash(self.individual_id + self.family_id + str(self.project))

class SeqrFamily:

    def __init__(
        self,
        family_id : str,
        project: BCHSeqrProject,
        index_sample : SeqrSample,
        mother_sample : Optional[SeqrSample],
        father_sample : Optional[SeqrSample],
        other_samples : Iterable[SeqrSample]
    ):
        samples = list(filter(None, [index_sample, mother_sample, father_sample])) + list(other_samples)

        # Validate that all the samples belong to the same project.
        project_set = set(
            map(
                lambda x: x.project,
                samples
            )
        )
        #if len(project_set) != 1:
        #    raise ValueError(f"SeqrFamily was made from individuals from non-unique projects: {project_set}")

        # The index sample is specifically labelled
        if index_sample.family_member_type != FamilyMemberType.Index:
            raise ValueError(f"SeqrFamily index_sample with individual_id {index_sample.individual_id} was not labelled an index")
        # Mother sample is specifically labelled
        if mother_sample:
            if mother_sample.family_member_type != FamilyMemberType.Mother:
                raise ValueError(f"SeqrFamily index_sample with individual_id {mother_sample.individual_id} was not labelled an index")
        # Father sample is specifically labelled
        if father_sample:
            if father_sample.family_member_type != FamilyMemberType.Father:
                raise ValueError(f"SeqrFamily index_sample with individual_id {father_sample.individual_id} was not labelled an index")
        # Make sure everyone belongs to the same family, and that the family_id is consistent.
        famid_set = set(
            map(
                lambda x: x.family_id,
                samples
            )
        )
        #if len(famid_set) != 1:
        #    raise ValueError(f"SeqrFamily had multiple family_ids: {famid_set}")
        #elif family_id not in famid_set:
        #    raise ValueError(f"SeqrFamily was assigned family_id {family_id} but the input samples had family_id {famid_set}")

        self.family_id = family_id
        self.index_sample = index_sample
        self.mother_sample = mother_sample
        self.father_sample = father_sample
        self.other_samples = set(other_samples)
        self.elasticsearch_index = index_sample.individual_id
        self.samples = samples

    @staticmethod
    def from_list_samples(inputlist: List[SeqrSample]):
        if not inputlist:
            return ValueError("inputlist in SeqrFamily.from_list_samples was empty")
        indexsample = None
        mothersample = None
        fathersample = None
        othersamples : Set[SeqrSample] = set()
        familyid = inputlist[0].family_id
        project = inputlist[0].project
        for sample in inputlist:
            if sample.family_id != familyid:
                return ValueError(f"Non-unique family_ids detected in SeqrFamily.from_list_samples: {familyid} and {sample.family_id} ")
            if sample.family_member_type == FamilyMemberType.Index:
                if indexsample:
                    # We want to *return* Exceptions, not raise them, to better handle errors.
                    return ValueError(f"Two index samples represented in SeqrFamily.from_list_samples: {indexsample.individual_id} and {sample.individual_id}")
                indexsample = sample
            elif sample.family_member_type == FamilyMemberType.Mother:
                if mothersample:
                    return ValueError(f"Two mother samples represented in SeqrFamily.from_list_samples: {mothersample.individual_id} and {sample.individual_id}")
                mothersample = sample
            elif sample.family_member_type == FamilyMemberType.Father:
                if fathersample:
                    return ValueError(f"Two father samples represented in SeqrFamily.from_list_samples: {fathersample.individual_id} and {sample.individual_id}")
                fathersample = sample
            else:
                if sample in othersamples:
                    return ValueError(f"Duplicate samples in SeqrFamily.from_list_samples: {sample.individual_id}")
                othersamples.add(sample)

        if not indexsample:
            return ValueError(f"No index sample provided in SeqrFamily.from_list_samples for family {familyid}")

        return SeqrFamily(
            familyid, project, indexsample,
            mothersample, fathersample, othersamples
        )
