from hashlib import md5
import json
from typing import Iterable


def hash_struct(data: Iterable) -> str:
    """
    Getting hash for iterable objects, including nested ones
    :param data: Iterable object of type dict, list or tuple
    :return: Hash of iterable object
    """
    if isinstance(data, dict):
        new_dict = dict()
        for key, value in data.items():
            if isinstance(value, (dict, list, tuple)):
                value = hash_struct(value)
            new_dict[key] = value
        simple_struct = tuple(sorted(new_dict.items(), key=lambda x: x[0]))
    elif isinstance(data, (list, tuple)):
        new_list = list()
        for value in data:
            if isinstance(value, (dict, list, tuple)):
                value = hash_struct(value)
            new_list.append(value)
        simple_struct = tuple(sorted(new_list))
    else:
        raise ValueError(f'{hash_struct.__name__} function is supposed for dict, list or tuple only')
    return md5(json.dumps(simple_struct).encode('utf-8')).hexdigest()
