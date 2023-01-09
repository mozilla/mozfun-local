from google.cloud import bigquery
from mozfun_local.mozfun_local_rust import glam_style_histogram as _glam_style_histogram
import polars as pl


def glam_style_histogram(
    probe: str, date: str, limit: int = None, dataset: str = "mozdata.telemetry.main"
) -> list:
    _limit = f"LIMIT {limit}" if limit else ""
    sql_query = f"""SELECT 
       client_id,
       application.build_id,
       {probe},
FROM {dataset}
WHERE date(submission_timestamp) = '{date}'
  AND date(submission_timestamp) > date(2022, 12, 20)
  AND {probe} IS NOT NULL
  {_limit}"""

    metadata = get_metadata(probe)

    project = dataset.split(".")[0]
    bq_client = bigquery.Client(project=project)

    dataset = bq_client.query(sql_query).result()
    df = pl.from_arrow(dataset.to_arrow())

    results = _glam_style_histogram(df, metadata)

    return results


def get_metadata(probe: str) -> str:
    # todo: read from file & find
    metadata = """{"probe": "wr_renderer_time", "histogram_type": "custom_distribution_exponential", "process": "parent", "probe_location": "payload.histograms.wr_renderer_time", "buckets_key": "min, max, n_buckets", "buckets_for_probe": [1, 1000, 50]}"""

    return metadata
