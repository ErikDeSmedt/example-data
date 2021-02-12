//! example-data is created to easily load common datasets.
//!
//! We use the [Apache Arrow](https://docs.rs/arrow/3.0.0/arrow/index.html) memory format
//! which allows for simple conversion to multiple dataframe implementations.
//!
//! ```rust
//! use example_data::{Repo};
//! use arrow::record_batch::RecordBatch;
//!
//! let iris = Repo::default().load_table("iris").unwrap();
//! let batches : Vec<RecordBatch> = iris.data();
//! let doc : &str = iris.doc().unwrap();
//! ```
//!
//! ### Polars Dataframes
//!
//! For Polars dataframes you can do
//!
//! ```rust
//! use example_data::{Repo};
//! use polars::frame::DataFrame;
//! use std::convert::TryFrom;
//!
//! let iris = Repo::default().load_table("iris").unwrap();
//! let polars : Result<DataFrame, _> = DataFrame::try_from(iris.data());
//! ```
//!
//! ### Supported datasets
//!
//! - iris
//! - boston
//!
//!
extern crate arrow;

mod datatables;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

/// A table with data
///
/// This corresponds to a DataFrame
#[derive(Clone, Debug)]
pub struct DataTable {
    batches: Vec<RecordBatch>,
    doc: Option<String>,
    name: String,
}

impl DataTable {
    /// The content of the DataTable
    ///
    /// It is guarnateed that all batches have
    /// exactly the same [`Schema`](arrow::datatypes::Schema)
    pub fn data(&self) -> Vec<RecordBatch> {
        // Note, that the cloning is not ridiculously expensive.
        //
        // we clone RecordBatches here
        // The data in all recordbatches is stored as an ArrayRef
        // We are just adding the counters in the Arc
        self.batches.iter().map(|x| x.clone()).collect()
    }

    /// The content of the DataTable
    ///
    /// It is guarnateed that all batches have
    /// exactly the same [`Schema`](arrow::datatypes::Schema)
    pub fn data_ref(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// The name of the DataTable
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The number of rows in the DataTable
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|x| x.num_rows()).sum()
    }

    /// The number of columns in the DataTable
    pub fn num_columns(&self) -> usize {
        self.batches[0].num_columns()
    }

    /// The schema of the DataTable
    pub fn schema(&self) -> SchemaRef {
        self.batches[0].schema()
    }

    /// The documentation of the DataTable
    pub fn doc(&self) -> Option<&str> {
        self.doc.as_deref()
    }
}

struct DataTableBuilder {
    batches: Option<Vec<RecordBatch>>,
    doc: Option<String>,
    name: Option<String>,
}

impl DataTableBuilder {
    fn new() -> Self {
        DataTableBuilder {
            batches: None,
            doc: None,
            name: None,
        }
    }

    fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    fn with_doc(mut self, doc: String) -> Self {
        self.doc = Some(doc);
        self
    }

    fn with_batches(mut self, batches: Vec<RecordBatch>) -> Self {
        self.batches = Some(batches);
        self
    }

    fn build(self) -> Result<DataTable, String> {
        let batches = self
            .batches
            .ok_or(String::from("Cannot create DataTable without data/batches"))?;
        let name = self
            .name
            .ok_or(String::from("Cannot create DataTable without a name."))?;

        let table = DataTable {
            name: name,
            batches: batches,
            doc: self.doc,
        };

        Ok(table)
    }
}

/// Repo is a collection of [`DataTable`](DataTable)s
pub trait Repo {
    /// Loads the [`DataTable`](DataTable) with matching name
    fn load_table(&self, name: &str) -> Result<DataTable, String>;
}

impl dyn Repo {
    /// Gets the default repository
    ///
    /// Currently, this is the only supported repository.
    /// In the current set-up all data-tables are included in the
    /// binary.
    ///
    /// This means that no network connection is required to connect to the Repo
    pub fn default() -> impl Repo {
        DefaultRepo {}
    }
}

struct DefaultRepo {}

impl Repo for DefaultRepo {
    /// Loads the [`DataTable`](DataTable) with corresponding name
    fn load_table(&self, name: &str) -> Result<DataTable, String> {
        match name {
            "iris" => crate::datasets::iris::load_table(),
            "boston" => crate::datasets::boston::load_table(),
            _ => Err(format!("{} could not be found in default-repository", name)),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use arrow::datatypes::DataType;

    #[test]
    fn test_can_load_iris() {
        let repo = Repo::default();
        let table: DataTable = repo.load_table("iris").unwrap();

        assert_eq!(
            table.num_rows(),
            150,
            "Iris is supposed to have 64 observations"
        );
        assert_eq!(
            table.num_columns(),
            5,
            "Iris is supposed to have 5 features"
        );

        // Checking field-names
        assert_eq!(table.schema().field(0).name(), "sepal_length");
        assert_eq!(table.schema().field(1).name(), "sepal_width");
        assert_eq!(table.schema().field(2).name(), "petal_length");
        assert_eq!(table.schema().field(3).name(), "petal_width");
        assert_eq!(table.schema().field(4).name(), "variety");

        // Checking field-names
        assert_eq!(table.schema().field(0).data_type(), &DataType::Float64);
        assert_eq!(table.schema().field(1).data_type(), &DataType::Float64);
        assert_eq!(table.schema().field(2).data_type(), &DataType::Float64);
        assert_eq!(table.schema().field(3).data_type(), &DataType::Float64);
        assert_eq!(table.schema().field(4).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_can_load_boston_housing() {
        let repo = Repo::default();
        let table: DataTable = repo.load_table("boston").unwrap();

        assert_eq!(table.num_rows(), 506);
        assert_eq!(table.num_columns(), 14);
    }
}
