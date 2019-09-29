from typing import List, Dict, Callable, Generic, TypeVar

T = TypeVar('T')
U = TypeVar('U')

def group_by(ls : List[T], mapping: Callable[[T],U]) -> Dict[U, List[T]]:
    retdict = {}
    for elt in ls:
        key = mapping(elt)
        if key in retdict:
            retdict[key].append(elt)
        else:
            retdict.update({key: [elt]})
    return retdict