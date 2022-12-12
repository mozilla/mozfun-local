use pyo3::{exceptions::PyTypeError, prelude::*};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

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

enum Distribution {
    TimingDistribution,
    MemoryDistribution,
    CustomDistributionExponential,
    CustomDistributionLinear,
}

impl FromStr for Distribution {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "timing_distribution" => Ok(Distribution::TimingDistribution),
            "memory_distribution" => Ok(Distribution::MemoryDistribution),
            "custom_distribution_exponential" => Ok(Distribution::CustomDistributionExponential),
            "custom_distribution_linear" => Ok(Distribution::CustomDistributionLinear),
            _ => Err("valid distribution type not provided"),
        }
    }
}

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
    buckets.insert(0);

    let max_bucket_id = sample_to_bucket_idx(
        range_max as f64,
        log_base as f64,
        buckets_per_magnitude as f64,
    );

    for idx in 0..max_bucket_id {
        let bucket =
            f64::powf(log_base as f64, idx as f64 / buckets_per_magnitude as f64).floor() as usize;
        if bucket < range_max {
            buckets.insert(bucket);
        } else {
            break;
        }
    }

    Vec::from_iter(buckets)
}

fn count_users(hist: &mut HashMap<usize, f64>) -> usize {
    // this is a neat trick; because all of the client histograms sum to
    // one, you can just sum the values to get N_reporting
    hist.values().sum::<f64>() as usize
}

fn transform_to_dirichlet_estimator(hist: &mut HashMap<usize, f64>, n_reporting: usize) {
    // Operates on a single histogram.
    // The histogram is filled at this point, can take K from length (total buckets)
    // -- Dirichlet distribution density for each bucket in a histogram.
    //  -- Given {k1: p1,k2:p2} where p’s are proportions(and p1, p2 sum to 1)
    //  -- return {k1: (P1+1/K) / (nreporting+1), k2:(P2+1/K) / (nreporting+1)}

    let k = hist.len() as f64;
    let n_reporting = n_reporting as f64;

    hist.iter_mut()
        .for_each(|(_, v)| *v = (*v + 1.0f64 / k) / n_reporting);
}

fn fill_buckets(hist: &mut HashMap<usize, f64>, buckets: &Vec<usize>) {
    buckets.iter().for_each(|x| {
        hist.entry(*x).or_insert(0f64);
    });
}

#[pyfunction]
pub fn calculate_dirichlet_distribution(
    histogram_vector: HashMap<usize, f64>,
    histogram_type: &str,
) -> PyResult<HashMap<usize, f64>> {
    // this will be a PyResult so mocking this for now
    // 1. aggregate client level histograms <- per client level
    // client_id || [bucket: sum, bucket: sum...]
    //
    // 2. normalize such that sum(values) = 1
    // client_id || [bucket: sum/total, bucket: sum/total...]
    //
    // 3. add all the normalized values together such that each bucket now sums
    // all of the normalized per-client values
    // [bucket: [u1_normalized, u2_normalized, ...]
    //
    // 4. the total sum of all values accross all buckets  equals the number reporting,
    // a handy coincidence of normalizing at the per client level
    //
    // 5. for every bucket that has a value, calculate the dirichelt approx transformation
    // using N_reporting from 4
    //
    // 6.generate the array of all bucket values we need to fill in and
    // add the dirichlet transfromed value (5) to the appropriate bucket
    //

    let mut hist = histogram_vector; // when we take the dictionary in from Python,
                                     // we probably cannot take it as a reference

    // assuming at this point I have aggregated, normalized histograms
    let n_reporting = count_users(&mut hist);
    let range_max = hist.keys().max().unwrap().clone(); // this histogram is about to get mutated,
                                                        // not sure this clone is necessary but it
                                                        // feels like it might be
    let _range_min = hist.keys().min().unwrap().clone(); // handle errors later
    let histogram_type = Distribution::from_str(histogram_type);

    let buckets = match histogram_type {
        Ok(Distribution::TimingDistribution) => generate_functional_buckets(2, 8, range_max),
        Ok(Distribution::MemoryDistribution) => generate_functional_buckets(2, 16, range_max),
        Ok(Distribution::CustomDistributionExponential) => todo!(),
        Ok(Distribution::CustomDistributionLinear) => todo!(),
        _ => return Err(PyErr::new::<PyTypeError, _>("Invalid Histogram Type")),
    };

    fill_buckets(&mut hist, &buckets);
    transform_to_dirichlet_estimator(&mut hist, n_reporting);

    Ok(hist)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_a_dirichlet() {
        let comp_hist: HashMap<usize, f64> = HashMap::from_iter([
            (0, (1.0 + (1.0 / 3.0)) / 3.0),
            (1, 1.0 / 9.0),
            (2, (2.0 + (1.0 / 3.0)) / 3.0),
        ]);

        let test_hist: HashMap<usize, f64> = HashMap::from_iter([(0, 1.0), (2, 2.0)]);

        let result = calculate_dirichlet_distribution(test_hist, "timing_distribution").unwrap();

        assert_eq!(comp_hist, result);
    }

    #[test]
    fn test_normalize_histogram() {
        let comp_hist: HashMap<usize, f64> = HashMap::from_iter([(2, 0.5), (11, 0.5)]);
        let input_hist: HashMap<usize, f64> = HashMap::from_iter([(11, 1.0), (2, 1.0)]);
        let test_hist = normalize_histogram(input_hist).unwrap();

        assert_eq!(comp_hist, test_hist)
    }
    #[test]
    fn test_generate_functional_buckets() {
        let mut buckets = generate_functional_buckets(2, 8, 305);

        let target: Vec<usize> = vec![
            0usize, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 19, 20, 22, 24, 26, 29,
            32, 34, 38, 41, 45, 49, 53, 58, 64, 69, 76, 82, 90, 98, 107, 117, 128, 139, 152, 165,
            181, 197, 215, 234, 256, 279, 304,
        ];

        buckets.sort();

        assert_eq!(target, buckets);
    }
}
