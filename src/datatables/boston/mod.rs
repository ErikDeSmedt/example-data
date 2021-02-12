use crate::{DataTable, DataTableBuilder};

use arrow::csv::{Reader, ReaderBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use std::io::Cursor;
use std::sync::Arc;

const BOSTON_DATA: &[u8] = include_bytes!("boston.csv");
const BOSTON_DOC: &[u8] = include_bytes!("boston_doc.txt");

pub fn load_table() -> Result<DataTable, String> {
    // Explicit specification of the schema
    let schema = Schema::new(vec![
        Field::new("CRIM", DataType::Float64, false),
        Field::new("ZN", DataType::Float64, false),
        Field::new("INDUS", DataType::Float64, false),
        Field::new("CHAS", DataType::Int64, false),
        Field::new("NOX", DataType::Float64, false),
        Field::new("RM", DataType::Float64, false),
        Field::new("AGE", DataType::Float64, false),
        Field::new("DIS", DataType::Float64, false),
        Field::new("RAD", DataType::Float64, false),
        Field::new("TAX", DataType::Float64, false),
        Field::new("PTRATIO", DataType::Float64, false),
        Field::new("B", DataType::Float64, false),
        Field::new("LSTAT", DataType::Float64, false),
        Field::new("MEDV", DataType::Float64, false),
    ]);

    let cursor = Cursor::new(BOSTON_DATA);

    let reader: Reader<_> = ReaderBuilder::default()
        .has_header(false)
        .with_schema(Arc::new(schema))
        .build(cursor)
        .expect("Can parse boston dataset");

    let batches: Result<Vec<RecordBatch>, _> = reader.collect();
    let batches = batches.expect("Internal error: Failed to read iris data-set");

    let doc: &str = std::str::from_utf8(BOSTON_DOC)
        .expect("Internal error: Failed to read boston-documentation");

    let table: DataTable = DataTableBuilder::new()
        .with_name(String::from("boston"))
        .with_doc(String::from(doc))
        .with_batches(batches)
        .build()
        .expect("Can read boston housing set");

    Ok(table)
}
