use std::collections::HashMap;

use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

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

#[derive(Serialize, Deserialize)]
struct GleanExperiments {
    experiments: Vec<GleanExperiment>,
}

#[derive(Serialize, Deserialize)]
struct GleanExperiment {
    key: String,
    value: GleanExperimentInfo,
}

#[derive(Serialize, Deserialize)]
struct GleanExperimentInfo {
    branch: String,
    extra: HashMap<String, String>,
}

#[pyfunction]
pub fn glean_legacy_compatible_experiments(
    experiment_data: &str,
) -> PyResult<HashMap<String, Vec<HashMap<String, String>>>> {
    let r: GleanExperiments = serde_json::from_str(experiment_data).unwrap();

    let experiments = r.experiments;

    let mut legacy_compatible_experiments = HashMap::new();
    let mut branch_info = Vec::new();

    for experiment in experiments {
        let mut legacy_map = HashMap::new();
        legacy_map.insert("key".to_string(), experiment.key);
        legacy_map.insert("value".to_string(), experiment.value.branch);

        branch_info.push(legacy_map);
    }

    legacy_compatible_experiments.insert("experiments".to_string(), branch_info);

    Ok(legacy_compatible_experiments)
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

    #[test]
    fn test_glean_legacy_compatible_experiments() {
        let data = r#"{  "experiments": [{    "key": "experiment_a",    "value": {      "branch": "control",      "extra": {        "type": "firefox"      }    }  }, {    "key": "experiment_b",    "value": {      "branch": "treatment",      "extra": {        "type": "firefoxOS"      }    }  }]}"#;

        let result = glean_legacy_compatible_experiments(data);
        let mut target = HashMap::new();

        target.insert(
            "experiments".to_string(),
            vec![
                HashMap::from([
                    ("key".to_string(), "experiment_a".to_string()),
                    ("value".to_string(), "control".to_string()),
                ]),
                HashMap::from([
                    ("key".to_string(), "experiment_b".to_string()),
                    ("value".to_string(), "treatment".to_string()),
                ]),
            ],
        );
        assert_eq!(result.unwrap(), target);
    }
}
