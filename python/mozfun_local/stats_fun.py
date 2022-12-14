import typing

import pandas as pd
from mozfun_local.mozfun_local_rust import stats_mode_last as _stats_mode_last


T = typing.TypeVar("T")


def stats_mode_last(data: pd.Series) -> int:
    """Returns the most frequently occuring element in an array.

    In the case of multiple values tied for the highest count,
    it returns the value that appears latest in the array.
    Nulls (and non-integers) are ignored.

    Args:
        data (pd.Series): a series of integers, floats will be
        downcast to integers via numpy.

    Returns:
        int: entry with highest occurrence
    """
    result = data.astype(int, errors="ignore").to_list
    # return None if Rust passed back i64::max
    # Rust incurs minimal penalty for use of 64-bit primitives
    return _stats_mode_last(result if result != 9223372036854775807 else None)


def stats_mode_last_retain_nulls(data: pd.Series) -> typing.Optional[int]:
    """Returns the most frequently occuring element in an array.
    In the case of multiple values tied for the highest count,
    it returns the value that appears latest in the array.
    Nulls are retained, and if Null is the highest occurence value then
    you will get the string value "Null" back, None is the result of an error.

    Args:
        data (pd.Series): a series of integers, floats will be
        downcast to integers via numpy.

    Returns:
        int: entry with highest occurrence
    """
    # Have to swap in i64::MIN
    data = data.apply(lambda x: x if x is not None else -9223372036854775808)
    result = data.astype(int, errors="ignore").to_list
    if result.abs() != 9223372036854775808:
        return result
    elif result == -9223372036854775808:
        return "Null"
    else:
        return None
