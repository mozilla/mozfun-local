#!/usr/bin/env python3
"""clients_daily_histogram_aggregates query generator."""
import argparse
import json
from pathlib import Path
import sys
from time import sleep
import urllib.request

from bigquery_etl.util import probe_filters
from google.cloud import bigquery

sys.path.append(str(Path(__file__).parent.parent.parent.resolve()))


PROBE_INFO_SERVICE = (
    "https://probeinfo.telemetry.mozilla.org/firefox/all/main/all_probes"
)

p = argparse.ArgumentParser()
p.add_argument(
    "--agg-type",
    type=str,
    help="One of histograms/keyed_histograms",
    required=True,
)
p.add_argument(
    "--json-output",
    action="store_true",
    help="Output the result wrapped in json parseable as an XCOM",
)
p.add_argument(
    "--wait-seconds",
    type=int,
    default=0,
    help="Add a delay before executing the script to allow time for the xcom sidecar to complete startup",
)
p.add_argument(
    "--processes",
    type=str,
    nargs="*",
    help="Processes to include in the output.  Defaults to all processes.",
)


def _get_keyed_histogram_sql(probes_and_buckets):
    probes = probes_and_buckets["probes"]
    buckets = probes_and_buckets["buckets"]

    probes_struct = []
    for probe, details in probes.items():
        for process in details["processes"]:
            probe_location = (
                f"payload.keyed_histograms.{probe}"
                if process == "parent"
                else f"payload.processes.{process}.keyed_histograms.{probe}"
            )
            buckets_for_probe = [
                buckets[probe]["min"],
                buckets[probe]["max"],
                buckets[probe]["n_buckets"],
            ]

            agg_string = (
                f"('{probe}', "
                f"'histogram-{details['type']}', "
                f"'{process}', "
                f"{probe_location}, "
                f"({buckets_for_probe}))"
            )

            hist_type = f"histogram-{details['type']}"
            agg_dict = {
                "probe": probe,
                "histogram_type": hist_type,
                "process": process,
                "probe_location": probe_location,
                "buckets_key": "min, max, n_buckets",
                "buckets_for_probe": buckets_for_probe,
            }

            print(json.dumps(agg_dict))

            probes_struct.append(agg_string)

    probes_struct.sort()
    probes_arr = ",\n\t\t\t".join(probes_struct)

    additional_queries = f"{probes_arr}"
    probs_string = ""
    windowed_clause = ""
    select_clause = ""

    return {
        "additional_queries": additional_queries,
        "select_clause": select_clause,
        "windowed_clause": windowed_clause,
    }


def get_histogram_probes_sql_strings(probes_and_buckets, histogram_type):
    """Put together the subsets of SQL required to query histograms."""
    probes = probes_and_buckets["probes"]
    buckets = probes_and_buckets["buckets"]

    sql_strings = {}
    if histogram_type == "keyed_histograms":
        return _get_keyed_histogram_sql(probes_and_buckets)

    probe_structs = []
    for probe, details in probes.items():
        for process in details["processes"]:
            probe_location = (
                f"payload.histograms.{probe}"
                if process == "parent"
                else f"payload.processes.{process}.histograms.{probe}"
            )
            buckets_for_probe = [
                buckets[probe]["min"],
                buckets[probe]["max"],
                buckets[probe]["n_buckets"],
            ]

            agg_string = (
                f"('{probe}', "
                f"'histogram-{details['type']}', "
                f"'{process}', "
                f"{probe_location}, "
                f"({buckets_for_probe}))"
            )

            hist_type = f"histogram-{details['type']}"
            agg_dict = {
                "probe": probe,
                "histogram_type": hist_type,
                "process": process,
                "probe_location": probe_location,
                "buckets_key": "min, max, n_buckets",
                "buckets_for_probe": buckets_for_probe,
            }

            print(json.dumps(agg_dict))

            probe_structs.append(agg_string)

    probe_structs.sort()
    probes_arr = ",\n\t\t\t".join(probe_structs)
    probes_string = f"""{probes_arr}"""
    sql_strings["select_clause"] = ""
    sql_strings["additional_queries"] = f"""{probes_string}"""

    sql_strings["windowed_clause"] = ""

    return sql_strings


def get_histogram_probes_and_buckets(histogram_type, processes_to_output):
    """Return relevant histogram probes."""
    project = "moz-fx-data-shared-prod"
    main_summary_histograms = {}

    client = bigquery.Client(project)
    table = client.get_table("telemetry_stable.main_v4")
    main_summary_schema = [field.to_api_repr() for field in table.schema]

    # Fetch the histograms field
    histograms_field = []
    for field in main_summary_schema:
        if field["name"] != "payload":
            continue

        for payload_field in field["fields"]:
            if payload_field["name"] == histogram_type:
                histograms_field.append(
                    {"histograms": payload_field, "process": "parent"}
                )
                continue

            if payload_field["name"] == "processes":
                for processes_field in payload_field["fields"]:
                    if processes_field["name"] in ["content", "gpu"]:
                        process_field = processes_field["name"]
                        for type_field in processes_field["fields"]:
                            if type_field["name"] == histogram_type:
                                histograms_field.append(
                                    {"histograms": type_field, "process": process_field}
                                )
                                break

    if len(histograms_field) == 0:
        return

    for histograms_and_process in histograms_field:
        for histogram in histograms_and_process["histograms"].get("fields", {}):
            if "name" not in histogram:
                continue

            processes = main_summary_histograms.setdefault(histogram["name"], set())
            if (
                processes_to_output is None
                or histograms_and_process["process"] in processes_to_output
            ):
                processes.add(histograms_and_process["process"])
            main_summary_histograms[histogram["name"]] = processes

    with urllib.request.urlopen(PROBE_INFO_SERVICE) as url:
        data = json.loads(url.read())
        excluded_probes = probe_filters.get_etl_excluded_probes_quickfix("desktop")
        histogram_probes = {
            x.replace("histogram/", "").replace(".", "_").lower()
            for x in data.keys()
            if x.startswith("histogram/")
        }

        bucket_details = {}
        relevant_probes = {
            histogram: {"processes": process}
            for histogram, process in main_summary_histograms.items()
            if histogram in histogram_probes and histogram not in excluded_probes
        }
        for key in data.keys():
            if not key.startswith("histogram/"):
                continue

            channel = "nightly"
            if "nightly" not in data[key]["history"]:
                channel = "beta"

                if "beta" not in data[key]["history"]:
                    channel = "release"

            data_details = data[key]["history"][channel][0]["details"]
            probe = key.replace("histogram/", "").replace(".", "_").lower()

            # Some keyed GPU metrics aren't correctly flagged as type
            # "keyed_histograms", so we filter those out here.
            if processes_to_output is None or "gpu" in processes_to_output:
                if data_details["keyed"] == (histogram_type == "histograms"):
                    try:
                        del relevant_probes[probe]
                    except KeyError:
                        pass
                    continue

            if probe in relevant_probes:
                relevant_probes[probe]["type"] = data_details["kind"]

            # NOTE: some probes, (e.g. POPUP_NOTIFICATION_MAINACTION_TRIGGERED_MS) have values
            # in the probe info service like 80 * 25 for the value of n_buckets.
            # So they do need to be evaluated as expressions.
            bucket_details[probe] = {
                "n_buckets": int(eval(str(data_details["n_buckets"]))),
                "min": int(eval(str(data_details["low"]))),
                "max": int(eval(str(data_details["high"]))),
            }

        return {"probes": relevant_probes, "buckets": bucket_details}


def main(argv):
    """Print a clients_daily_histogram_aggregates query to stdout."""
    opts = vars(p.parse_args(argv[1:]))

    if opts["agg_type"] in ("histograms", "keyed_histograms"):
        probes_and_buckets = get_histogram_probes_and_buckets(
            opts["agg_type"], opts["processes"]
        )
        get_histogram_probes_sql_strings(probes_and_buckets, opts["agg_type"])
    else:
        raise ValueError("agg-type must be one of histograms, keyed_histograms")

    sleep(opts["wait_seconds"])


if __name__ == "__main__":
    main(sys.argv)
