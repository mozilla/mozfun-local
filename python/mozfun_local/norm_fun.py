import typing
from datetime import date, datetime
from datetime import timedelta

import pandas as pd
import numpy as np

from numba import jit

from mozfun_local.mozfun_local_rust import norm_normalize_os as _norm_normalize_os
from mozfun_local.mozfun_local_rust import VersionTruncator, VersionExtractor

truncator = None
extractor = None


def norm_normalize_os(os: str) -> str:
    """Normalize an operating system string to one of the three major desktop platforms,
    one of the two major mobile platforms, or "Other".
    Reimplementation of logic used in the data pipeline:
    https://github.com/mozilla/gcp-ingestion/blob/a6928fb089f1652856147c4605df715f327edfcd/ingestion-beam/src/main/java/com/mozilla/telemetry/transforms/NormalizeAttributes.java#L52-L74

    Args:
        os (str): the operating system field. If the value is not found
        'other' will be returned.

    Returns:
        str: normalized operating system.

    """
    return _norm_normalize_os(os)


def norm_truncate_version(
    raw_version: str, truncate_to_version: str
) -> typing.Optional[int]:
    """Extracts numeric version data from a version string like <major>.<minor>.<patch>.

    Note: Non-zero minor and patch versions will be floating point Numeric.

    Args:
        raw_version (str): string version to truncate
        truncate_to_version (str): "major" or "minor"

    Returns:
        typing.Optional[int]: an integer representing the past of the version
        that you have specified. Returns None if something goes wrong
    """
    truncate_to_version = truncate_to_version.lower()
    assert truncate_to_version in [
        "major",
        "minor",
    ], f"{truncate_to_version} is neither major/minor"

    global truncator
    truncator = VersionTruncator()

    if truncate_to_version == "major":
        return truncator.find_major_version(raw_version)
    if truncate_to_version == "minor":
        return truncator.find_minor_version(raw_version)
    else:
        return None


def norm_extract_version(
    raw_version: str, part_to_extract: str
) -> typing.Optional[int]:
    """Extract a given part of the version, either Major, Minor or Patch version

    Args:
        raw_version (str): string representation of the version.
        part_to_extract (str): which part of the version you want retunred.

    """

    part_to_extract = part_to_extract.lower()
    assert part_to_extract in [
        "major",
        "minor",
        "patch",
    ], f"{part_to_extract} was not one of major/minor/patch"

    global extractor
    if extractor is None:
        extractor = VersionExtractor()

    return extractor.extract_version(raw_version, part_to_extract)


def norm_glean_fenix_build_to_date(app_build: str, format: str = "datetime"):
    """Convert the Fenix client_info.app_build-format string to a DATETIME. Returns None on failure.

    Fenix originally used an 8-digit app_build format. Newer builds use a 10-digit format.

    This function tolerates both formats.

        Args:
            app_build (str): app_build in string format. Will be cast to string if not.
            format (str): provide "date" to get a date, or "datetime" to get a dattime. Defauls to "datetime"
    """
    if type(app_build) != str:
        app_build = str(app_build)

    appbuild_len = len(app_build)
    if appbuild_len not in [
        8,
        10,
    ]:
        return None

    try:
        # need to do some bitwise operations here
        # first cast to int64
        i64_app_build = np.int64(int(app_build))
    except:
        return None

    if appbuild_len == 8:
        if int(app_build[4:6]) >= 24 or int(app_build[6:]) >= 60:
            return None
        corrected_year = int(app_build[0]) + 2018

        builddate = datetime(
            corrected_year, 1, 1, int(app_build[4:6]), int(app_build[6:])
        )
        builddate = builddate + timedelta(days=int(app_build[1:4]) - 1)
        return builddate.date() if format == "date" else builddate

    # Branchless, this is the 10 digit section
    base_date = datetime(2014, 12, 28, 0, 0, 0)

    shifted_app_build = _bitwise_shift_eight_chars(i64_app_build)

    return base_date + timedelta(hours=float(shifted_app_build))


@jit(nopython=True)
def _bitwise_shift_eight_chars(x: np.int64):
    # shift left then right to drop all but 20 rightmost bits
    # 64-20 = 44
    x = x << 44 >> 44

    # now drop the last 3 of those
    x = x >> 3

    return x
