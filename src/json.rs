use std::collections::HashMap;

use pyo3::prelude::*;
use serde::Deserializer;
use serde_json::{Result, Value};

#[pyfunction]
pub fn json_mode_last(data: Vec<&str>) -> PyResult<String> {
    // Json version of mode_last, pyo3 will coerce types to strings
    // One-pass mode calculation
    // Source: https://codereview.stackexchange.com/a/173437
    if data.is_empty() {
        return Ok("".to_string());
    }
    let mut occurence_map = HashMap::new();

    // max_by_key Returns the element that gives the maximum value from the specified function.
    // Returns the second argument if the comparison determines them to be equal.
    // So this conforms to mode_last
    let highest_occurrence = data.iter().copied().max_by_key(|&i| {
        let occurences = occurence_map.entry(i).or_insert(0);
        *occurences += 1;
        *occurences
    });

    Ok(highest_occurrence.unwrap().to_string())
}

#[pyfunction]
pub fn json_extract_string_map(input: &str) -> PyResult<Vec<HashMap<String, String>>> {
    let result: Vec<HashMap<String, String>> = serde_json::from_str(input).unwrap_or_default();

    // let result_map: Vec<HashMap<String, String>> = Vec::new();

    // if result.len() == 0 {
    //     return Ok(vec!(HashMap { "None", "None"}));
    // }

    // for element in result {

    // }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_mode_last() {
        // See stats::test_mode_last for more tests
        let string_vec = vec!["thing1", "thing2", "thing1"];

        assert_eq!(json_mode_last(string_vec).unwrap(), "thing1");
    }
}
