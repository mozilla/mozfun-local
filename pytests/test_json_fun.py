from pandas import DataFrame
import pytest
from mozfun_local.json_fun import (
    json_mode_last,
    json_extract_int_map,
    json_extract_string_map,
)


def test_json_extract_int_map():
    list_of_arrays = """[
        {"key": "0", "value": "12434"},
        {"key": "1", "value": "297"},
        {"key": "13", "value": "8"},
    ]"""

    crunched_list = json_extract_int_map(list_of_arrays)
    assert crunched_list == [{0: 12434}, {1: 297}, {13: 8}]

    # empty array
    assert json_extract_int_map([]) is None

    # empty struct inside of a list
    assert json_extract_int_map([{}]) is None
    # assert.array_equals(
    # [STRUCT(1 AS key, NULL AS value)],
    assert json_extract_int_map([{"key": 1, "value": "Null"}]) is None

    with pytest.raises(TypeError):
        thing = json_extract_int_map({"1": 147573952589676410000})

    thing = json_extract_int_map([{"key": "1", "value": 147573952589676410000}])


def test_json_extract_string_map():
    data = """{"a":"text","b":1,"c":null,"d":{},"e":[]}"""

    post_fix_data = [
        {"key": "a", "value": "text"},
        {"key": "b", "value": "1"},
        {"key": "c", "value": None},
        {"key": "d", "value": "{}"},
        {"key": "e", "value": "[]"},
    ]

    assert json_extract_string_map(data) == post_fix_data


def test_json_mode_last():
    data_0 = DataFrame(["foo", "bar", "baz", "bar"], columns=["test"])
    assert json_mode_last(data_0) == "bar"

    data_1 = DataFrame(["foo", "bar", "baz", "bar", "baz", "fred"], columns=["test"])
    assert json_mode_last(data_1) == "baz"
    data_2 = DataFrame(["foo", None, None], columns=["test"])
    assert json_mode_last(data_2) == "foo"
