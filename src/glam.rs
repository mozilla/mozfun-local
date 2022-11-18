use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};

fn sample_to_bucket_idx(sample: f64, log_base: f64, buckets_per_magnitude: f64) -> usize {
    let exponent = f64::powf(log_base, 1f64 / buckets_per_magnitude);
    f64::ceil((sample + 1.0).ln() / exponent.ln()) as usize
}
fn generate_functional_buckets(
    log_base: usize,
    buckets_per_magnitude: usize,
    range_max: usize,
) -> Vec<usize> {
    let mut buckets = HashSet::with_capacity(range_max);
    buckets.insert(0usize);

    let max_bucket_id = sample_to_bucket_idx(
        range_max as f64,
        log_base as f64,
        buckets_per_magnitude as f64,
    );

    for idx in 0..max_bucket_id {
        let bucket =
            f64::powf(log_base as f64, idx as f64 / buckets_per_magnitude as f64).floor() as usize;
        if bucket > range_max {
            buckets.insert(bucket);
        } else {
            break;
        }
    }

    Vec::from_iter(buckets)
}

fn fill_histogram_from_dirichlet(hist: Vec<(i64, i64)>, nreporting: i64) -> Vec<(i64, f64)> {
    // histograms filled at this point, can take K from length (total buckets)
    // -- Dirichlet distribution density for each bucket in a histogram.
    //  -- Given {k1: p1,k2:p2} where p’s are proportions(and p1, p2 sum to 1)
    //  -- return {k1: (P1+1/K) / (nreporting+1), k2:(P2+1/K) / (nreporting+1)}
    let K = hist.len() as i64;

    let dirichlet = hist
        .iter()
        .map(|m| (m.0, (m.1 + 1f64 / K) / (K + 1)))
        .collect::<Vec<_>>();

    dirichlet
}

#[allow(dead_code)]
#[pyfunction]
pub fn aggregate_histogram(histogram_vector: Vec<String>) -> PyResult<Vec<i64>> {
    // aggregate client level histograms
    // make a histogram from those aggregations
    // calculate dirichlet from that using the len as the n observed
    // :check: create all the buckets we need to return with generate_functional_buckets
    // :todo: parse out the sample to fit in those
    // data type: list of dicts<string, string OR int> {"key": "1234" [string], "value": 1234 [int]

    // calculate total number of clients
    let N = histogram_vector.len();

    // generate entire set of buckets needed
    let out = generate_functional_buckets(log_base, buckets_per_magnitude, range_max);

    // need to do this for the entire client set
    // for histogram in histogram_vector
    // for each interior histogram fill with any bucket that doesn't already exist
    // todo: investigate merging instead of sequential insert
    // I think I want to convert these to vec tuple though, not hashmaps as they have single kv
    // pairs
    for i in out {
        if !histogram_vector.contains(&i) {
            histogram_vector.insert(i, 0);
        }
    }

    Ok(vec![0, 1, 2, 3])
}
