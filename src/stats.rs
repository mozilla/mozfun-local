use std::collections::HashMap;

use pyo3::prelude::*;

/// Sum of a groupby of keys
#[pyfunction]
pub fn mode_last(data: Vec<i64>) -> PyResult<i64> {
    // One-pass mode calculation
    // Source: https://codereview.stackexchange.com/a/173437
    if data.is_empty() {
        return Ok(i64::MAX);
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

    Ok(highest_occurrence.unwrap())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_mode_last() {
        let well_formed = vec![1, 1, 2, 1, 3, 2, 1];
        let shared_mode = vec![1, 1, 2, 2, 2, 1];
        let shared_mode_opposite = vec![1, 1, 2, 2, 1, 2];
        let empty_vec: Vec<i64> = vec![];
        let python_nulls = vec![1, 1, i64::MIN, i64::MIN, i64::MIN];

        assert_eq!(mode_last(well_formed).unwrap(), 1);
        assert_eq!(mode_last(shared_mode).unwrap(), 1);
        assert_eq!(mode_last(shared_mode_opposite).unwrap(), 2);
        assert_eq!(mode_last(empty_vec).unwrap(), i64::MAX);
        assert_eq!(mode_last(python_nulls).unwrap(), i64::MIN);
    }
}
