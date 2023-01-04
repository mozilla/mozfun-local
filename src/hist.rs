use pyo3::prelude::*;
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
