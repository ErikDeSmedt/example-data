# Example-data

This crate provides toy-datasets to be used in rust. Because we are using the [Apache Arrow](https://arrow.apache.org/) memory format it can easily be used for multiple dataframe implementations.

Currently, all datasets are embedded in the library. 

## Usage
```rust
use example_data::{Repo};
use arrow::record_batch::RecordBatch;

let iris = Repo::default().load_table("iris").unwrap();
let batches : Vec<RecordBatch> = iris.data();
let doc : &str = iris.doc().unwrap();
```

## Suported data

- **iris**: Classification if iris-flowers by petal- and sepal size
- **boston**: Prediction of housing price based on accessibility, pollution, ...

## Contribute
We use github. If you have any questions feel free to open an issue or send a pull-request. We'll be happy to assist.
