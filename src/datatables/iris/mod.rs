use crate::{DataTable, DataTableBuilder};

use arrow::csv::{Reader, ReaderBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use std::io::Cursor;
use std::sync::Arc;

const IRIS_DATA: &[u8] = include_bytes!("iris.csv");
const IRIS_DOC: &[u8] = include_bytes!("iris_doc.txt");

pub fn load_table() -> Result<DataTable, String> {
    // Specify the schema explicitly to avoid suprises
    let schema = Schema::new(vec![
        Field::new("sepal_length", DataType::Float64, false),
        Field::new("sepal_width", DataType::Float64, false),
        Field::new("petal_length", DataType::Float64, false),
        Field::new("petal_width", DataType::Float64, false),
        Field::new("variety", DataType::Utf8, false),
    ]);

    // Loading the iris dataset
    let cursor = Cursor::new(IRIS_DATA);
    let reader: Reader<_> = ReaderBuilder::default()
        .has_header(true)
        .with_schema(Arc::new(schema))
        .build(cursor)
        .expect("Internal error: Failed to load iris data-set");

    let batches: Result<Vec<RecordBatch>, _> = reader.collect();
    let batches = batches.expect("Internal error: Failed to parse iris dataframe");

    // Loading the documentation of the iris dataset
    let doc: &str =
        std::str::from_utf8(IRIS_DOC).expect("Internal error: Failed to read iris documentation");

    let data_table: DataTable = DataTableBuilder::new()
        .with_name(String::from("iris"))
        .with_batches(batches)
        .with_doc(String::from(doc))
        .build()
        .expect("InternalError: Failed to construct iris datatable");

    Ok(data_table)
}
