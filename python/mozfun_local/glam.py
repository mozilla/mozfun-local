from google.cloud import bigquery
from mozfun_local.mozfun_local_rust import glam_style_histogram as _glam_style_histogram
import polars as pl
from pathlib import Path
import os


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


_metadata = metadata()


def glam_style_histogram(
    probe: str,
    keyed: bool,
    date: str,
    limit: int = None,
    dataset: str = "mozdata.telemetry.main_1pct",
) -> list:
    _limit = f"LIMIT {limit}" if limit else ""
    probe_location = (
            f"payload.histograms.{probe}" if not keyed else f"payload.keyed_histograms.{probe}"
    )
    sql_query = f"""SELECT 
       client_id,
       application.build_id,
       {probe_location},
FROM {dataset}
WHERE date(submission_timestamp) = '{date}'
  AND date(submission_timestamp) > date(2022, 12, 20)
  AND {probe_location} IS NOT NULL
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
    global _metadata
    return _metadata.metadata[probe]
