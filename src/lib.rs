mod decimal_format;
mod parser;
mod schema;

use anyhow::Error;
pub use decimal_format::*;
pub use parser::*;
pub use schema::*;

pub fn parser(config: parser::ParserConfig) -> Result<Parser, Error> {
    parser::Parser::new(config)
}

#[cfg(test)]
mod tests {
    use std::thread;

    use crate::schema;
    use crossbeam::channel::{unbounded, Receiver, Sender};
    use rayon::iter::{ParallelBridge, ParallelIterator};

    use super::*;

    #[test]
    fn test_fixedwidthschema() {
        let schema: schema::Schema =
            schema::Schema::new("./example/fixedwidth_schema.xml").expect("Failed to load schema");

        assert!(schema.fixedwidthschema.is_some());
    }

    #[test]
    fn test_parser() {
        let config = crate::ParserConfig {
            file_path: "./example/fixedwidth_data.txt".to_string(),
            file_schema: "./example/fixedwidth_schema.xml".to_string(),
        };

        let mut parser = crate::parser(config).unwrap();

        for line_result in parser.iter_mut() {
            match line_result {
                Ok(processed_line) => println!("{:?}", processed_line),
                Err(processed_line) => println!("Error processing line: {:?}", processed_line),
            }
        }
    }

    #[test]
    fn test_parser_thread() {
        let config = crate::ParserConfig {
            file_path: "./example/fixedwidth_data.txt".to_string(),
            file_schema: "./example/fixedwidth_schema.xml".to_string(),
        };

        let n_workers = 4;

        let mut parser = crate::parser(config).unwrap();

        type LineNumberAndText = (usize, String);

        let (sender, receiver): (Sender<LineNumberAndText>, Receiver<LineNumberAndText>) = unbounded();
        let mut handles = vec![];
        for _ in 0..n_workers {
            let receiver = receiver.clone();
            let schema = parser.schema.clone();

            handles.push(thread::spawn(move || {
                let mut return_errors: Vec<ProcessedLineError> = Vec::new();
                for (line_number, line_content) in receiver {
                    match schema.validate_line(line_number, line_content) {
                        Ok(_) => {}
                        Err(v) => {
                            return_errors.push(ProcessedLineError { line_number: v.line_number, message: v.message });
                        }
                    }
                }
                return_errors
            }));
        }

        let lines = parser.lines();

        for line_result in lines {
            let line_result = match line_result {
                Ok(line_result) => line_result,
                Err(err) => {
                    println!("Error reading line: {:?}", err);
                    continue;
                }
            };
            let result = sender.send((line_result.line_number, line_result.line_content.to_owned()));
            match result {
                Ok(_) => {}
                Err(err) => {
                    println!("Error sending line to worker thread: {:?}", err);
                    continue;
                }
            }
        }
        drop(sender);

        let mut return_errors: Vec<ProcessedLineError> = Vec::new();
        for handle in handles {
            let results = handle.join().expect("Failed to join thread");
            for result in results {
                return_errors.push(result);
            }
        }

        if !return_errors.is_empty() {
            println!("Errors: {:?}", return_errors);
        }
    }

    #[test]
    fn test_parser_iter_par() {
        let config = crate::ParserConfig {
            file_path: "./example/fixedwidth_data.txt".to_string(),
            file_schema: "./example/fixedwidth_schema.xml".to_string(),
        };

        let mut parser = crate::Parser::new(config).unwrap();
        let schema = parser.schema.clone();

        parser
            .lines()
            .par_bridge()
            .map(|read_line| {
                match read_line {
                    Ok(read_line) => {
                        let line_number = read_line.line_number;
                        let line_content = read_line.line_content;

                        // TEST: sleep for 10 seconds on line 3 to test parallel processing
                        if line_number == 3 {
                            std::thread::sleep(std::time::Duration::from_secs(10));
                        }

                        match schema.validate_line(line_number, line_content.to_owned()) {
                            Ok(processed_line) => Ok(processed_line),
                            Err(processed_line) => Err(processed_line),
                        }
                    }
                    Err(e) => Err(ProcessedLineError { line_number: 0, message: format!("{}", e) }),
                }
            })
            .for_each(|result_processed_line| match result_processed_line {
                Ok(processed_line) => {
                    println!("{:?}", processed_line);
                }
                Err(processed_line) => {
                    println!("{:?}", processed_line);
                }
            });
    }

    #[test]
    fn test_validate_number() {
        // # is optional digit, 0 is required digit
        let pattern = "0,##0.00;(#,##0.000)";
        let formatter = decimal_format::DecimalFormat::new(pattern).unwrap();
        assert!(formatter.validate_number("2,234.56").is_ok());
        assert!(formatter.validate_number("-1,234.560").is_ok());
        assert!(formatter.validate_number("1234.56").is_err());
        assert!(formatter.validate_number("1234").is_err());

        let pattern = "0.#0,##0";
        let formatter = decimal_format::DecimalFormat::new(pattern).unwrap();
        assert!(formatter.validate_number("2.20,125").is_ok());

        let pattern = "';#'##0";
        let formatter = decimal_format::DecimalFormat::new(pattern).unwrap();
        assert!(formatter.validate_number(";#123").is_ok());

        let pattern = "#######0.00";
        let formatter = decimal_format::DecimalFormat::new(pattern).unwrap();
        assert!(formatter.validate_number("00204000.00").is_ok());
    }
}
