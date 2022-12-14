import typing

import pandas as pd
from mozfun_local.mozfun_local_rust import map_sum as _map_sum
from mozfun_local.mozfun_local_rust import map_get_key as _map_get_key


T = typing.TypeVar("T")


def map_sum(data: pd.Series, reversed: bool = False) -> pd.DataFrame:
    result = pd.DataFrame(_map_sum(data))
    if reversed:
        return result.sort_index(ascending=True)
    else:
        return result.sort_index(ascending=False)


def map_get_key(
    s: str, key: str, trim_chars: bool = True, coerce_to_number: str = None
) -> T:
    """Fetch the value associated with a given key from an array of
    key/value structs. Because map types aren't available in BigQuery,
    we model maps as arrays of structs instead, and this function
    provides map-like access to such fields. Returns empty string "" if
    key not found or another proble occurs.


    Args:
        s (str): a string encoded bigquery struct. It is assumed that
        you have a valid array<struct>, in json format, from bigquery.
        Can handle curly braces and whitespace chars with use of trim_chars.
        trim_chars (str, optional): whether to trim leading/trailing characters.
        Can be set to False for a small performance increase if you know your
        arrays are well formed
        key (str): a string that represents the key for which you desire to
        retrieve the value in the struct
        coerce_to_number (str, optional): attempt to return numerical value
        instead of string. Choices are "int" and "float".
        Other values will throw, as will failed coercians
        (consistent with sql) Defaults to None.

    Returns:
        T: string if uncoerced, int or float if specified"""
    valid_formats = [None, "float", "int"]
    assert (
        coerce_to_number.lower() in valid_formats
    ), f"{coerce_to_number.lower()} not a valid choice (please supply None\
    , 'float' or 'int')"
    result = _map_get_key(s, key, trim_chars)

    if coerce_to_number == "int":
        return int(result)
    elif coerce_to_number == "float":
        return float(result)
    else:
        return result


def map_get_key_with_null(s: str, key: str, trim_chars: bool) -> T:
    """Because we are not in SQL, we just handle null chars as strings.

    This calls map_get_key internally, but with no coercian to int"""
    return map_get_key(s, key, trim_chars)
