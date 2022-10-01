from datetime import datetime

import numpy as np

from mozfun_local.norm_fun import norm_glean_fenix_build_to_date


def test_norm_glean_fenix_build_to_date_old_style_typical_date():
    assert datetime.strptime(
        "2020-06-05 14:34:00", "%Y-%m-%d %H:%M:%S"
    ) == norm_glean_fenix_build_to_date("21571434")

    assert datetime.strptime(
        "2018-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"
    ) == norm_glean_fenix_build_to_date("00010000")

    assert datetime.strptime(
        "2027-12-31 23:59:00", "%Y-%m-%d %H:%M:%S"
    ) == norm_glean_fenix_build_to_date("93652359")

    assert datetime.strptime(
        "2020-08-13 04:00:00", "%Y-%m-%d %H:%M:%S"
    ) == norm_glean_fenix_build_to_date("2015757667")

    assert datetime.strptime(
        "2014-12-28 00:00:00", "%Y-%m-%d %H:%M:%S"
    ) == norm_glean_fenix_build_to_date("0000000000")

    assert datetime.strptime(
        "2014-12-28 00:00:00", "%Y-%m-%d %H:%M:%S"
    ) == norm_glean_fenix_build_to_date("0000000001")

    assert datetime.strptime(
        "2014-12-28 00:00:00", "%Y-%m-%d %H:%M:%S"
    ) == norm_glean_fenix_build_to_date(str(np.int64(1) << 31))

    assert None == norm_glean_fenix_build_to_date("7777777")

    assert None == norm_glean_fenix_build_to_date("999999999")

    assert None == norm_glean_fenix_build_to_date("3")

    assert None == norm_glean_fenix_build_to_date("hi")

    assert None == norm_glean_fenix_build_to_date("11831860")

    assert None == norm_glean_fenix_build_to_date("11832459")
