from typing import Optional, Dict, List
import ast


def glean_timespan_nanos(
    timespan: Dict[str, int],
    key_key: str = "time_unit",
    value_key: str = "value",
) -> Optional[int]:
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
    timespan: dict,
    key_key: str = "time_unit",
    value_key: str = "value",
) -> Optional[int]:
    """Returns the number of seconds represented by a Glean timespan struct,
    rounded down to full seconds.
    See https://mozilla.github.io/glean/book/user/metrics/timespan.html

    Args:
        timespan (Dict[str, int]): input struct, will be coerced
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
        return int(timespan[value_key] / 1000 / 1000)
    if unit_of_time == "millisecond":
        return int(timespan[value_key] / 1000)
    if unit_of_time == "second":
        return int(timespan[value_key])
    if unit_of_time == "minute":
        return int(timespan[value_key] * 60)
    if unit_of_time == "hour":
        return int(timespan[value_key] * 60 * 60)
    if unit_of_time == "day":
        return int(timespan[value_key] * 60 * 60 * 24)
    return None


def glean_legacy_compatible_experiments(experiment_data) -> Dict[str, list]:
    """Parse experiments from the newer Glean experiment format into the format used by Legacy Telementy. If you provide a string it will be coerced via literal_eval, if that fails returns None.

    Args:
        experiment_data: Experiment data, the form it comes in from in BQ is valid. Not provided in signature because it's too complicated.

    Returns:
        Dict[experiment_name, branch_name]
    """
    if type(experiment_data) == str:
        experiment_data = ast.literal_eval(experiment_data)

    experiments = experiment_data[0]["experiments"]

    experiment_list = []
    for experiment in experiments:
        experiment_list.append(
            {"key": experiment["key"], "value": experiment["value"]["branch"]}
        )

    experiment_dict = {"experiments": experiment_list}

    return experiment_dict
