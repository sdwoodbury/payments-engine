use csv::{self, ReaderBuilder};
use error_stack::Result;
use payments_engine::{
    errors::print_report, errors::*, transaction_processor::TransactionProcessor,
};
use std::{fs, io::BufReader, path::Path, process::ExitCode};

fn main() -> ExitCode {
    env_logger::init();
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("error: no input file specified");
        return ExitCode::FAILURE;
    }

    let input_file = &args[1];

    // ensure the item exists
    let path = Path::new(input_file);
    if !path.exists() {
        eprintln!("error: \"{}\" does not exist", input_file);
        return ExitCode::FAILURE;
    }

    // ensure the item is a file
    if !path.is_file() {
        eprintln!("error: {} is not a file", input_file);
        return ExitCode::FAILURE;
    }

    // attempt to open the file
    let open_res = fs::OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .open(input_file);

    if open_res.is_err() {
        eprintln!("failed to open file: {}", open_res.unwrap_err());
        return ExitCode::FAILURE;
    }

    // unwrap() is guaranteed to not panic
    match process_transactions(open_res.unwrap()) {
        Err(e) => {
            print_report(e);
            ExitCode::FAILURE
        }
        Ok(_) => ExitCode::SUCCESS,
    }
}

fn process_transactions(input_file: fs::File) -> Result<(), MyError> {
    let mut processor = TransactionProcessor::new()?;

    // process the input file, skippipping records with invalid formats.
    let reader = BufReader::new(input_file);
    let mut csv_reader = ReaderBuilder::new().from_reader(reader);
    for mut string_record in csv_reader.records().flatten() {
        string_record.trim();
        // deserialize it, skip invalid formats
        if let Ok(txn) = string_record.deserialize(None) {
            processor.process(txn)?;
        }
    }
    processor.display()?;
    Ok(())
}
