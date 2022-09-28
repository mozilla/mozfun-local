use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;

use pyo3::prelude::*;
use rayon::prelude::*;

use serde_json::Value;

/// Sum of a groupby of keys
#[pyfunction]
pub fn map_sum(r: Vec<(&str, f64)>) -> PyResult<HashMap<&str, f64>> {
    let result_map = DashMap::new();
    let arc_result_map = Arc::new(result_map);

    r.par_iter().for_each(|x| {
        let arc_result_map_clone = arc_result_map.clone();
        let _ = *arc_result_map_clone
            .entry(x.0)
            .and_modify(|y| *y += x.1)
            .or_insert(x.1);
    });

    let unwraped_result_map = Arc::try_unwrap(arc_result_map).expect("rc not zero");

    // this could be less than great with a very large number of keys, but then why?
    Ok(unwraped_result_map
        .into_iter()
        .collect::<HashMap<&str, f64>>())
}

/// Parse a string with json array in it
#[pyfunction]
pub fn map_get_key(input: &str, key: &str, trim: bool) -> PyResult<String> {
    let trim_chars = ['{', '}', '\n', '\r', ' '];
    let cleaned_string = match trim {
        true => input
            .trim_start_matches(trim_chars)
            .trim_end_matches(trim_chars),
        false => input,
    };

    let r: Value = serde_json::from_str(cleaned_string).unwrap_or_default();

    let len = match r.is_array() {
        true => r.as_array().unwrap().len(),
        false => 0,
    };
    for i in 0..len {
        if r[i]["key"] == key {
            let out = r[i]["value"].to_string();
            return Ok(out
                .trim_start_matches('\"')
                .trim_end_matches('\"')
                .to_string());
        }
    }

    Ok("".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    #[test]
    fn test_map_sum() {
        let input = vec![
            ("thing1", 1f64),
            ("thing1", 2f64),
            ("thing1", 3f64),
            ("thing1", 4f64),
            ("thing2", 2f64),
            ("thing2", 2f64),
            ("thing2", 2f64),
            ("thing2", 2f64),
            ("thing2", 2f64),
            ("thing2", 2f64),
            ("thing2", 2f64),
            ("thing2", 2f64),
            ("thing3", 144f64),
            ("thing3", 144f64),
            ("thing3", 144f64),
            ("thing3", 144f64),
        ];
        let thing: PyResult<HashMap<&str, f64>> = map_sum(input);
        let thing_result = &thing.unwrap();
        assert_eq!(thing_result.get("thing1").unwrap(), &10f64);
        assert_eq!(thing_result.get("thing2").unwrap(), &16f64);
        assert_eq!(thing_result.get("thing3").unwrap(), &576f64);
    }

    #[test]
    fn test_map_get_key() {
        let data = r#"{
            [{
              "key": "foo",
              "value": "42"
            }, {
              "key": "bar",
              "value": "12"
            }]
          }"#;
        let r_data = r#"[{
            "key": "foo",
            "value": "42"
          }, {
            "key": "bar",
            "value": "12"
          }]"#;
        // let result = map::map_get_key(data, "baz");

        assert_eq!(map_get_key(data, "foo", true).unwrap(), "42");
        assert_eq!(map_get_key(data, "foo", false).unwrap(), "");
        assert_eq!(map_get_key(r_data, "bar", false).unwrap(), "12");
        assert_eq!(map_get_key(data, "baz", true).unwrap(), "");
    }
}
