import os
from pathlib import Path

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
    probe: str,
    keyed: bool,
    date: str,
    limit: int = None,
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
    table -- full path to the table you wish to take probes from (default mozdata.telemetry.main_1pct)
    """
    _limit = f"LIMIT {limit}" if limit else ""
    probe_location = (
        f"payload.histograms.{probe}"
        if not keyed
        else f"payload.keyed_histograms.{probe}"
    )
    sql_query = f"""SELECT 
       client_id,
       application.build_id,
       {probe_location},
FROM {table}
WHERE date(submission_timestamp) = '{date}'
  AND date(submission_timestamp) > date(2022, 12, 20)
  AND {probe_location} IS NOT NULL
  {_limit}"""

    metadata = get_metadata(probe)

    project = table.split(".")[0]
    bq_client = bigquery.Client(project=project)

    dataset = bq_client.query(sql_query).result()
    df = pl.from_arrow(dataset.to_arrow())

    results = _glam_style_histogram(df, metadata)

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
    percentiles = sorted(percentiles) # we only need to go through once
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


def calculate_percentiles(distribution: list, percentiles: list) -> dict:
    """Given a list of percentiles and a distribution, find the buckets that
    represent each percentile. Internal functions are numba jitted python.

    Keyword Arguments:
    distribution -- list of key-value pairs, these are already sorted in
                    mozfun-local's rust function
    percentiles -- list of floating point values [0.0, 1.0] of the percentiles
                   you wish to calculate
    """
    if type(percentiles) != np.ndarray:
        percentiles = np.array(percentiles)

    k, v = _lists_from_tuples(distribution)

    cumulative_distribution = np.cumsum(v)

    cutoffs = _find_cutoffs(k, cumulative_distribution, percentiles)

    return cutoffs
