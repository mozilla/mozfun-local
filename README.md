# mozfun-local

mozfun-local is a python (for now) package that is a canonical local version of the functionality provided by the very excellent [mozfun](https://mozilla.github.io/bigquery-etl/mozfun/about/) for your local environment (rather than as BigQuery UDFs), built using primarily Rust and Numpy. These are primarily designed for cases when you have already done an expensive query and would find it easier to modify the data locally than to rerun your query.

Typically speaking, executing things on BQ will be faster, but when that's not the most practical, this package may be of some use.

On occasion, Rust's concurrency model is used for operations that lend themselves well to parallelization. Or, there are some cases where functions in mozfun are udfs written in javascript, where compiled Rust has been used here and performance may actually be better.

Numba just in time compilation is also used where sensible.

All functions provide _at least_ the same functionality as their BigQuery equivalents, while a few offer some additionals (which will be noted). All capability here is tested against the same set of tests used by BigQuery


## In use

Function naming follows the snake_case pattern {group}_{function}, for example: json_extract_int_map would correspond to [extract_int_map in JSON](https://mozilla.github.io/bigquery-etl/mozfun/json/)

Python is very permissive w.r.t. datatypes; _in general_ you can assume that functions will provide functionality only on types that conform to BQ types, but occasionally there is expanded capability. All functions are typehinted and commented where possible, and a python language server will likely make your experience easier.

## Is Rust really faster than just using Numpy?

If your variables are not already in Numpy datatypes, typically yes. Consider the case when we have a string that we need to process into an int64 to perform bitwise operations on (an actual usecase from mozfun):

Numpy: ```np.int64(x) << 44 >> 44 << 3```

Numba JIT: .0363ms

Rust: ```x.parse::<i64>().unwrap() << 44 >> 44 << 3```

Release: .0344ms

Obviously this isn't much difference, so it really comes down to which paradigm you prefer. JITed Numpy is certainly easy to write, though whether it's behaving to your expectation can be hard to intuit. Rust, while less terse, will do exactly what you tell it. Plus, if Numba fails back, it  will be MUCH slower, and this failure may not be trivial to work around.
## Requirements

* Rust >= 1.60
* Python >= 3.8 (3.10 recommended)
* Maturin
* (recommended) PyEnv or (not recommended) Conda

## Building

This package can be built by invoking ```maturin develop --release``` in the top level folder.

## Testing

Testing rust: ```cargo test --no-default-features:w```

Testing python: ```python -m pytest pytests/*```

TODO: test coverage stats
