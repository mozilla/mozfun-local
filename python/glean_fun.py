import typing
import ast


def glean_timespan_nanos(
    timespan: typing.Dict[str, int],
    key_key: str = "time_unit",
    value_key: str = "value",
) -> int:
    """Returns the number of nanoseconds represented by a Glean timespan struct.
    See https://mozilla.github.io/glean/book/user/metrics/timespan.html

    Args:
        timespan (typing.Dict[str, int]): input struct, will be coerced
        into a dictionary if provided in str form.
        key_key (str, optional): Key for the struct, rarely needs to be changed.
        Defaults to "time_unit".
        value_key (str, optional): Value key for the struct, rarely needs to be
        change. Defaults to "value".


    Returns:
        int: seconds, integer rounded down
    """

    unit_of_time = timespan[key_key]

    if unit_of_time == "nanosecond":
        return int(timespan[value_key])
    if unit_of_time == "microsecond":
        return int(timespan[value_key] * 1000)
    if unit_of_time == "millisecond":
        return int(timespan[value_key] * 1000 * 1000)
    if unit_of_time == "second":
        return int(timespan[value_key] * 1000 * 1000 * 1000)
    if unit_of_time == "minute":
        return int(timespan[value_key] * 1000 * 1000 * 1000 * 60)
    if unit_of_time == "hour":
        return int(timespan[value_key] * 1000 * 1000 * 1000 * 60 * 60)
    if unit_of_time == "day":
        return int(timespan[value_key] * 1000 * 1000 * 1000 * 60 * 60 * 24)
    return None


def glean_timespan_seconds(
    timespan: typing.Dict[str, int],
    key_key: str = "time_unit",
    value_key: str = "value",
) -> int:
    """Returns the number of seconds represented by a Glean timespan struct,
    rounded down to full seconds.
    See https://mozilla.github.io/glean/book/user/metrics/timespan.html

    Args:
        timespan (typing.Dict[str, int]): input struct, will be coerced
        into a dictionary if provided in str form.
        key_key (str, optional): Key for the struct, rarely needs to be changed.
        Defaults to "time_unit".
        value_key (str, optional): Value key for the struct, rarely needs to be
        change. Defaults to "value".


    Returns:
        int: seconds, integer rounded down
    """
    if type(timespan) == str:
        try:
            dict(ast.literal_eval(timespan))
        except SyntaxError:
            return None

    unit_of_time = timespan[key_key]

    if unit_of_time == "nanosecond":
        return int(timespan[value_key] / 1000 / 1000 / 1000)
    if unit_of_time == "microsecond":
        return int(timespan[value_key] * 1000)
    if unit_of_time == "millisecond":
        return int(timespan[value_key] * 1000 * 1000)
    if unit_of_time == "second":
        return int(timespan[value_key] * 1000 * 1000 * 1000)
    if unit_of_time == "minute":
        return int(timespan[value_key] * 1000 * 1000 * 1000 * 60)
    if unit_of_time == "hour":
        return int(timespan[value_key] * 1000 * 1000 * 1000 * 60 * 60)
    if unit_of_time == "day":
        return int(timespan[value_key] * 1000 * 1000 * 1000 * 60 * 60 * 24)
    return None
