use pyo3::prelude::*;

pub mod bytes;
pub mod glam;
pub mod hist;
pub mod json;
pub mod map;
pub mod norm;
pub mod stats;

// Remember to decorate with #[pyfunction]
/// A Python module implemented in Rust.
#[pymodule]
fn mozfun_local_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(map::map_sum, m)?)?;
    m.add_function(wrap_pyfunction!(map::float_map_sum, m)?)?;
    m.add_function(wrap_pyfunction!(map::int_map_sum, m)?)?;
    m.add_function(wrap_pyfunction!(map::map_get_key, m)?)?;
    m.add_function(wrap_pyfunction!(stats::mode_last, m)?)?;
    m.add_function(wrap_pyfunction!(json::json_mode_last, m)?)?;
    m.add_class::<norm::Matcher>()?;
    m.add_class::<norm::Extractor>()?;
    m.add_function(wrap_pyfunction!(norm::norm_normalize_os, m)?)?;
    m.add_function(wrap_pyfunction!(bytes::bytes_bit_pos_to_byte_pos, m)?)?;
    m.add_function(wrap_pyfunction!(
        json::glean_legacy_compatible_experiments,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(hist::normalize_histogram, m)?)?;
    m.add_function(wrap_pyfunction!(glam::test_runner, m)?)?;

    Ok(())
}
