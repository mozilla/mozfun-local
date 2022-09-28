# mozfun-local

mozfun-local is a python (for now) package that aims to bring the functionality provided by
the very excellent [mozfun](https://mozilla.github.io/bigquery-etl/mozfun/about/) to your local environment. These are primarily designed for cases when you have already done an expensive query and would find it easier to modify the data locally than to rerun your query.

Typically speaking, executing things on BQ will be faster, but when that's not the best answer, this package may be.

On occasion, Rust's concurrency model is used for operations that lend themselves well to parallelization. Or, there are some cases where functions in mozfun are udfs written in javascript, where compiled Rust has been used here and performance may actually be better.

## In use

Function naming follows the snake_case pattern {group}_{function}, for example: json_extract_int_map would correspond to [extract_int_map in JSON](https://mozilla.github.io/bigquery-etl/mozfun/json/)

Python is very permissive w.r.t. datatypes; *in general* you can assume that functions will provide functionality only on types that conform to BQ types, but occasionally there is expanded capability. All functions are typehinted and commented, and a python language server will likely make your experience easier.

## Requirements

* Rust >= 1.60
* Python >= 3.8 (3.10 recommended)
* Maturin
* (recommended) PyEnv or (not recommended) Conda

## Building

This package can be built by invoking ```maturin develop``` in the top level folder.

## Testing

WIP