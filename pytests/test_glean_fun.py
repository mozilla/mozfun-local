from mozfun_local.glean_fun import (
    glean_legacy_compatible_experiments,
    glean_timespan_nanos,
    glean_timespan_seconds,
)


def test_glean_legacy_experiments():
    data = [
        {
            "experiments": [
                {
                    "key": "experiment_a",
                    "value": {"branch": "control", "extra": {"type": "firefox"}},
                },
                {
                    "key": "experiment_b",
                    "value": {"branch": "treatment", "extra": {"type": "firefoxOS"}},
                },
            ]
        }
    ]

    print(glean_legacy_compatible_experiments(data))

    fixed_data = {
        "experiments": [
            {"key": "experiment_a", "value": "control"},
            {"key": "experiment_b", "value": "treatment"},
        ]
    }

    result = glean_legacy_compatible_experiments(data)
    assert fixed_data == result

    result_python = glean_legacy_compatible_experiments(data, rust=False)
    assert fixed_data == result


def test_glean_timespan_nanos():
    assert 345_600_000_000_000 == glean_timespan_nanos({"time_unit": "day", "value": 4})
    assert 13 == glean_timespan_nanos({"time_unit": "nanosecond", "value": 13})
    assert glean_timespan_nanos({"time_unit": "nonexistent_unit", "value": 13}) is None


def test_glean_timespan_seconds():
    assert 345_600 == glean_timespan_seconds({"time_unit": "day", "value": 4})
    assert 0 == glean_timespan_seconds({"time_unit": "nanosecond", "value": 13})
    assert 13 == glean_timespan_seconds({"time_unit": "second", "value": 13})
    assert (
        glean_timespan_seconds({"time_unit": "nonexistent_unit", "value": 13}) is None
    )
