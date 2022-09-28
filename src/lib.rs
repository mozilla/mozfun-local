use pyo3::prelude::*;

pub mod json;
pub mod map;
pub mod norm;
pub mod stats;

// Remember to decorate with #[pyfunction]
/// A Python module implemented in Rust.
#[pymodule]
fn mozfun_local(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(map::map_sum, m)?)?;
    m.add_function(wrap_pyfunction!(map::map_get_key, m)?)?;
    m.add_function(wrap_pyfunction!(stats::mode_last, m)?)?;
    m.add_function(wrap_pyfunction!(json::json_mode_last, m)?)?;
    m.add_class::<norm::Matcher>()?;
    m.add_class::<norm::Extractor>()?;
    m.add_function(wrap_pyfunction!(norm::norm_normalize_os, m)?)?;
    Ok(())
}
