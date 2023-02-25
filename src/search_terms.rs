use std::{collections::HashMap, str::FromStr};

use pyo3::prelude::*;

enum risk {
    Numerics,
    AtSign,
    Names,
    NoIssue,
}

impl FromStr for risk {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut chars = s.chars();
        let result = if chars.any(|c| c.is_numeric()) {
            Ok(risk::Numerics)
        } else if chars.any(|c| c == '@') {
            Ok(risk::AtSign)
        } else {
            Ok(risk::NoIssue)
        };

        result
    }
}

fn string_rep(r: &risk) -> String {
    match r {
        risk::Numerics => "contains_numbers".to_string(),
        risk::AtSign => "contains_@".to_string(),
        risk::Names => "contains_names".to_string(),
        risk::NoIssue => "no_issues".to_string(),
    }
}

fn calculate_mask(t: risk) -> bool {
    match t {
        risk::NoIssue => false,
        _ => true,
    }
}

#[pyfunction]
pub fn sanitize_query(queries: Vec<&str>) -> PyResult<(Vec<bool>, HashMap<String, usize>)> {
    // we know exactly which entries we want, and that defaults should be zero
    // the repeated to_string calls are due to python not being able to easily
    // handle &str when we pass our data back to it.
    let mut sanitization_hist = HashMap::from([
        ("contains_numbers".to_string(), 0),
        ("contains_@".to_string(), 0),
        ("contains_names".to_string(), 0),
        ("no_issues".to_string(), 0),
    ]);

    let mask = queries
        .iter()
        .map(|q| risk::from_str(q))
        .map(|r| {
            let r = r.unwrap();
            let k = string_rep(&r);
            sanitization_hist.entry(k).and_modify(|v| *v += 1);
            r // silly trick but for_each returns ()
        })
        .map(|r| calculate_mask(r))
        .collect::<Vec<bool>>();

    Ok((mask, sanitization_hist))
}
