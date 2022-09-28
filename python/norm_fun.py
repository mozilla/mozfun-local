import typing

import pandas as pd

from mozfun_local import norm_normalize_os as _norm_normalize_os
from mozfun_local import VersionTruncator, VersionExtractor

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
        return truncator.find_minor_version(raw_version)
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


if __name__ == "__main__":
    print(norm_normalize_os("Android"))
    print(norm_truncate_version("106.0.1", "minor"))
    print(norm_extract_version("106.0.1", "patch"))
