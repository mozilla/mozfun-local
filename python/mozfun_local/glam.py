import os
from pathlib import Path
from datetime import datetime

from google.cloud import bigquery
from mozfun_local.mozfun_local_rust import glam_style_histogram as _glam_style_histogram
import numpy as np
import polars as pl


class metadata:
    ### Reads metadata for use with GLAM histogram processing
    ###
    def __init__(self) -> None:
        self.metadata = self._read_metadata()

    def _read_metadata(self) -> dict:
        metadata = {}
        package_directory = Path(os.path.dirname(__file__)).parent
        utils = os.path.join(package_directory, "utils/")
        files = [f for f in os.listdir(utils) if ".txt" in f]
        files = [os.path.join(utils, f) for f in files]

        for file in files:
            with open(file, "r") as f:
                for line in f.readlines():
                    probe = line.split(",", maxsplit=1)[0].split(" ")[1][1:-1]
                    metadata[probe] = line.strip()

        return metadata


# This is global & instantiated on import to avoid re-reads. This is the worst
# solution, except for all of the others (passing around a big metadata class,
# making it explicit, etc.
_metadata = metadata()


def glam_style_histogram(
    probes: list,
    keyed: bool,
    date: str,
    limit: int = None,
    sample_rate: float = None,
    table: str = "mozdata.telemetry.main_1pct",
) -> list:
    """Calculate the GLAM style histogram transformation to a given histogram
    metric. The result is a list of sorted key-value pairs of bucket and the
    dirichlet distribution estimator at that bucket (non-cumulative). From this,
    percentiles can be calculated using the calculate_percentiles function.

    Keyword Arguments:
    probe -- string of the probe you wish to calculate (e.g. wr_renderer_time)
    keyed -- bool if the histogram is keyed
    date -- string of date you wish to calculate the transformation for (date is a partition key)
    limit -- int of the number of rows from the ping to take (default None/no limit)
    sample_rate -- float what percent of samples to take per histogram, starting at zero, only divisible by 20, default None
    table -- full path to the table you wish to take probes from (default mozdata.telemetry.main_1pct)
    """
    scaled_sample_rate = sample_rate * 100 if sample_rate else 20
    if sample_rate is not None:
        assert (
            (scaled_sample_rate) % 20 == 0 and sample_rate > 0.0 and sample_rate <= 1.0
        ), "sample rate must be between zero and one, and divsible by 20 in whole number representation"

    _limit = f"LIMIT {limit}" if limit else ""

    results = []
    probe_locations = [
        (
            f"payload.histograms.{probe}"
            if not keyed
            else f"payload.keyed_histograms.{probe}"
        )
        for probe in probes
    ]
    print(f"got metadata for probes at {datetime.now()}")

    probe_string = (", \n    ").join(probe_locations)

    # 0 <= sample_id < 100
    for f in np.arange(20, scaled_sample_rate + 10, 20):
        if sample_rate:
            sample_id_string = (
                f"AND sample_id >= {int(f - 10)} AND sample_id < {int(f)}"
            )
        else:
            sample_id_string = ""
        sql_query = f"""SELECT 
    client_id,
    application.build_id,
    {probe_string},
FROM {table}
WHERE date(submission_timestamp) = '{date}'
AND date(submission_timestamp) > date(2022, 12, 20)
{sample_id_string}
{_limit}"""

        project = table.split(".")[0]
        bq_client = bigquery.Client(project=project)

        print(f"starting query at {datetime.now()}")
        dataset = bq_client.query(sql_query).result()
        print(f"got data from bq at {datetime.now()}")
        data = pl.from_arrow(dataset.to_arrow())  # type: pl.DataFrame

        for probe in probes:
            df = data.select(["client_id", "build_id", probe])
            df = df.filter(pl.col(probe).is_not_null())

            metadata = get_metadata(probe)
            print(f"sending {probe} to Rust at {datetime.now()}")
            result = _glam_style_histogram(df, metadata)
            results.append((probe, f, f - 10, result))
            print(f"finished through sample_id {f} at {datetime.now()}")

    return results


def get_metadata(probe: str) -> str:
    global _metadata
    return _metadata.metadata[probe]


def _lists_from_tuples(tuples):
    """Makes two separate lists from a list of tuples."""
    # this is hideous but empirically faster than zip(*l) and append()
    k, v = [k for k, _ in tuples], [v for _, v in tuples]

    return k, v


def _find_cutoffs(buckets, cdf, percentiles):
    assert len(percentiles) > 0, "Must provide at least one percentile to calculate"
    percentiles = sorted(percentiles)  # we only need to go through once
    # if values are sorted

    results = {}
    max_iter = len(cdf)
    i = 0
    for p in percentiles:
        while i < max_iter and cdf[i] < p:
            i += 1
        if i < max_iter:
            results[p] = buckets[i]
        else:
            results[p] = buckets[-1]

    return results


def calculate_percentiles(distribution, percentiles) -> dict:
    """Given a list of percentiles and a distribution, find the buckets that
    represent each percentile. Internal functions are numba jitted python.

    Keyword Arguments:
    distribution -- list of key-value pairs, these are already sorted in
                    mozfun-local's rust function
    percentiles -- list of floating point values [0.0, 1.0] of the percentiles
                   you wish to calculate
    """
    # perf improvement as we iterate later and numpy has known types
    if type(percentiles) != np.ndarray:
        percentiles = np.array(percentiles)  # type: np.ndarray

    k, v = _lists_from_tuples(distribution)

    cumulative_distribution = np.cumsum(v)

    cutoffs = _find_cutoffs(k, cumulative_distribution, percentiles)

    return cutoffs
