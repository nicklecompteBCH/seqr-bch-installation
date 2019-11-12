from typing import (
    List, Dict, Callable,
    Generic, TypeVar, Iterable
)

T = TypeVar('T')
U = TypeVar('U')

def group_by(ls : Iterable[T], mapping: Callable[[T],U]) -> Dict[U, List[T]]:
    retdict : Dict[U, List[T]] = {}
    for elt in ls:
        key = mapping(elt)
        if key in retdict:
            retdict[key].append(elt)
        else:
            retdict.update({key: [elt]})
    return retdict

def indefinite_or(input_list:list) -> bool:
    """Not sure if Python had this.

    :param input_list: A list of Boolable items.
    :type input_list: list, each element must implement __bool__
    :return: [description]
    :rtype: bool
    """
    for item in input_list:
        if item:
            return True
    return False

def is_structlike(input_object):
    """Helper for teasing through Python's memory semantics.

    :param input_object: [description]
    :type input_object: [type]
    :return: [description]
    :rtype: [type]
    """
    if indefinite_or(map(lambda x : isinstance(input_object,x), [int, str, float])):
        return True

def deep_copy(input_object):
    """A deep copy that isn't mysterious,
    and is shakily promised to fail rather than pretend to work.

    :param input_object: Something which hopefully has a deep_copy or self.deep_copy() implementation.
    :type input_object: Whatever is in the list below.
    :raises ValueError: [description]
    :return: [description]
    :rtype: [type]
    """
    if is_structlike(input_object):
        y = input_object
        return y
    if isinstance(input_object,set):
        retset = set()
        for item in input_object:
            retset.add(deep_copy(item))
        return retset
    if isinstance(input_object, list):
        retlist = []
        for item in input_object:
            retlist.append(item)
        return retlist
    if isinstance(input_object,dict):
        retdict = {}
        for key in input_object:
            retdict.update({deep_copy(key) : deep_copy(input_object[key])})
        return retdict
    else:
        try:
            return input_object.deep_copy()
        except AttributeError:
            raise ValueError(f"input_object did not seem to have a deep_copy implementation.")

