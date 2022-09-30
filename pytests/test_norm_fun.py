from datetime import datetime

import numpy as np

from mozfun_local.norm_fun import norm_glean_fenix_build_to_date


def test_norm_glean_fenix_build_to_date_old_style_typical_date():
    assert datetime.strptime(
        "2020-06-05 14:34:00", "%Y-%m-%d %H:%M:%S"
    ) == norm_glean_fenix_build_to_date("21571434")


def test_norm_glean_fenix_build_to_date_old_style_short_increment():
    assert datetime.strptime(
        "2018-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"
    ) == norm_glean_fenix_build_to_date("00010000")


def test_norm_glean_fenix_build_to_date_old_style_future_date():
    assert datetime.strptime(
        "2027-12-31 23:59:00", "%Y-%m-%d %H:%M:%S"
    ) == norm_glean_fenix_build_to_date("93652359")


def test_norm_glean_fenix_build_to_date_new_style_normal():
    assert datetime.strptime(
        "2020-08-13 04:00:00", "%Y-%m-%d %H:%M:%S"
    ) == norm_glean_fenix_build_to_date("2015757667")


def test_norm_glean_fenix_build_to_date_new_style_zero():
    assert datetime.strptime(
        "2014-12-28 00:00:00", "%Y-%m-%d %H:%M:%S"
    ) == norm_glean_fenix_build_to_date("0000000000")


def test_norm_glean_fenix_build_to_date_new_style_one():
    assert datetime.strptime(
        "2014-12-28 00:00:00", "%Y-%m-%d %H:%M:%S"
    ) == norm_glean_fenix_build_to_date("0000000001")


def test_norm_glean_fenix_build_to_date_shift():
    assert datetime.strptime(
        "2014-12-28 00:00:00", "%Y-%m-%d %H:%M:%S"
    ) == norm_glean_fenix_build_to_date(str(np.int64(1) << 31))


def test_norm_glean_fenix_build_to_date_old_style_invalid():
    assert None == norm_glean_fenix_build_to_date("7777777")


def test_norm_glean_fenix_build_to_date_new_style_invalid():
    assert None == norm_glean_fenix_build_to_date("999999999")


def test_norm_glean_fenix_build_to_date_too_short():
    assert None == norm_glean_fenix_build_to_date("3")


def test_norm_glean_fenix_build_to_date_string():
    assert None == norm_glean_fenix_build_to_date("hi")


def test_norm_glean_fenix_build_to_date_out_of_bounds_seconds():
    assert None == norm_glean_fenix_build_to_date("11831860")


def test_norm_glean_fenix_build_to_date_out_of_bounds_hours():
    assert None == norm_glean_fenix_build_to_date("11832459")
