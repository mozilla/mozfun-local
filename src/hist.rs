use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[pyfunction]
pub fn normalize_histogram(hist: HashMap<usize, f64>) -> PyResult<HashMap<usize, f64>> {
    // Normalization of histogram. New values will be the existing value
    // divided by the total accross all buckets.
    let total: f64 = hist.values().sum();

    Ok(hist
        .iter()
        .map(|(k, v)| (*k, *v / total))
        .collect::<HashMap<usize, f64>>())
}

#[derive(Serialize, Deserialize)]
struct MainHistogram {
    bucket_count: usize,
    histogram_type: usize,
    sum: usize,
    range: Vec<usize>,
    values: HashMap<String, i64>,
}

fn parse_data_json(s: &str) -> Result<MainHistogram, serde_json::error::Error> {
    serde_json::from_str(s)
}

impl MainHistogram {
    fn clamp_keys(self) -> HashMap<i64, i64> {
        let max_value = 2i64.pow(40);
        self.values
            .into_iter()
            .map(|(k, v)| (k.parse::<i64>().unwrap(), v))
            .filter(|(k, _)| *k < max_value)
            .collect()
    }
}

#[derive(Serialize, Deserialize)]
struct HistogramMetaData {
    probe: String,
    histogram_type: String,
    process: String,
    probe_location: String,
    buckets_key: String,
    buckets_for_probe: Vec<usize>,
}

fn parse_metadata_json(s: &str) -> Result<HistogramMetaData, serde_json::error::Error> {
    serde_json::from_str(s)
}

pub fn parse_main_histograms(v: Vec<String>) -> Vec<HashMap<i64, i64>> {
    v.into_iter()
        .map(|s| parse_data_json(s.as_str()).unwrap().clamp_keys())
        .collect()
}
