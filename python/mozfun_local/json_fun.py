import typing
import ast

import pandas as pd

from mozfun_local.mozfun_local_rust import json_mode_last as _json_mode_last


def json_mode_last(data: pd.Series) -> int:
    """Returns the most frequently occuring element in an array.

    In the case of multiple values tied for the highest count,
    it returns the value that appears latest in the array.

    This version considers all primitives (int, str, float, etc),
    use the stats version for a version that only considers
    ints (which will be more performant).

    Args:
        data (pd.Series): a series of integers, floats will be
        downcast to integers via numpy.

    Returns:
        int: entry with highest occurrence
    """
    # result = data.astype(int, errors="ignore").to_list
    # return None if Rust passed back i64::max
    # Rust incurs minimal penalty for use of 64-bit primitives
    return _json_mode_last(data)


def _inner_struct_cast(item):
    if item["value"] == "Null":
        return None
    return {int(item["key"]): int(item["value"])}


def json_extract_int_map(
    array_of_structs: typing.List[str],
) -> typing.Optional[typing.List[dict[int, int]]]:
    """Returns an array of key/value structs from a string representing a JSON map.
    Both keys and values are cast to integers.

    This is the format for the "values" field in the desktop telemetry histogram
    JSON representation.

    This casts to dict[int, int], and does so with Python list comprehension.
    If you pass in a str this function will attempt to evaluate it into a list,
    but this requires that the first character be '['. If evaluation fails,
    an empty dictionary will be returned.

    Args:
        array_of_structs (typing.List[str]): _description_

    Returns:
        typing.List[dict[int, int]]: _description_
    """
    if type(array_of_structs) == list:
        if len(array_of_structs) == 0 or array_of_structs[0] == {}:
            return None
    elif type(array_of_structs) != str:
        raise (
            TypeError(
                f"this function supports lists and raw json in lists, you provided {type(array_of_structs)}"
            )
        )
    if type(array_of_structs) == str:
        first_character = array_of_structs[0]
        assert (
            first_character == "["
        ), f"first character must be '[', but found '{first_character}' instead"
        try:
            array_of_structs = ast.literal_eval(
                array_of_structs
            )  # typing.List[str, str]
        except SyntaxError:
            return {}
    result = [_inner_struct_cast(d) for d in array_of_structs]
    return result if result[0] is not None else None
