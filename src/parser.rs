use anyhow::Context;
use anyhow::Error;
use anyhow::Result;

use crossbeam::channel::Receiver;
use indexmap::map::IndexMap;
use std::fs::File;

use std::io::{BufRead, BufReader};

use crate::schema;

pub type WorkerFunction =
    fn(Receiver<(usize, String)>, schema::Schema) -> Vec<Result<ProcessedLineOk, ProcessedLineError>>;

#[derive(Debug)]
pub struct ProcessedLineOk {
    pub line_number: usize,
    pub cell_values: IndexMap<String, String>,
    pub linetype: String,
}

#[derive(Debug)]
pub struct ProcessedLineError {
    pub line_number: usize,
    pub message: String,
}

#[derive(Debug)]
pub struct ReadLine {
    pub line_number: usize,
    pub line_content: String,
}

#[derive(Debug)]
pub struct ParserConfig {
    pub file_path: String,
    pub file_schema: String,
}

#[derive(Debug)]
struct FileBuffer<R: BufRead> {
    reader: R,
    current_line: usize,
    newline_characters: Vec<u8>, // The newline characters used to separate lines
    buf: Vec<u8>,
    finished: bool,
}

#[derive(Debug)]
pub struct Parser {
    pub config: ParserConfig,
    pub schema: schema::Schema,
    file_buffer: FileBuffer<BufReader<File>>, // File buffer for reading lines from the input file
}

impl<R: BufRead> FileBuffer<R> {
    fn new(reader: R, newline_characters: String) -> Self {
        Self {
            reader,
            current_line: 0,
            newline_characters: newline_characters.into_bytes(),
            buf: Vec::new(),
            finished: false,
        }
    }
}

/// This implementation is specialized for reading lines from a file with custom newline characters.
impl<R: BufRead> Iterator for FileBuffer<R> {
    type Item = std::io::Result<ReadLine>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished && self.buf.is_empty() {
            return None;
        }

        let newline_characters_str = match String::from_utf8(self.newline_characters.to_owned()) {
            Ok(v) => v,
            Err(e) => return Some(Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e))),
        };

        let mut newline_characters_bytes = Vec::new();
        let mut chars = newline_characters_str.chars();
        while let Some(ch) = chars.next() {
            if ch == '\\' {
                match chars.next() {
                    Some('n') => newline_characters_bytes.push(b'\n'),
                    Some('r') => newline_characters_bytes.push(b'\r'),
                    Some('t') => newline_characters_bytes.push(b'\t'),
                    Some('f') => newline_characters_bytes.push(b'\x0C'),
                    Some('0') => newline_characters_bytes.push(0),
                    Some(other) => newline_characters_bytes.push(other as u8),
                    None => break,
                }
            } else {
                newline_characters_bytes.push(ch as u8);
            }
        }

        let mut match_index = 0;

        loop {
            let mut byte = [0; 1];
            match self.reader.read_exact(&mut byte) {
                Ok(()) => {
                    self.buf.push(byte[0]);
                    if byte[0] == newline_characters_bytes[match_index] {
                        match_index += 1;
                        if match_index == newline_characters_bytes.len() {
                            self.buf.truncate(self.buf.len() - newline_characters_bytes.len());
                            break;
                        }
                    } else {
                        match_index = 0;
                    }
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        self.finished = true;
                        break;
                    } else {
                        return Some(Err(e));
                    }
                }
            }
        }

        let line = match String::from_utf8(self.buf.clone()) {
            Ok(line) => line,
            Err(e) => return Some(Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e))),
        };
        self.current_line += 1;
        self.buf.clear();
        Some(Ok(ReadLine { line_number: self.current_line, line_content: line }))
    }
}

/// The `Parser` struct represents a parser for a specific file format.
/// It provides methods for initializing the parser, iterating over the lines of the file,
/// and processing each line according to a specified schema.
impl Parser {
    /// Creates a new `Parser` instance with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration for the parser, including the file path and schema.
    ///
    /// # Returns
    ///
    /// A `Result` containing the `Parser` instance if successful, or an `Error` if an error occurred.
    pub fn new(config: ParserConfig) -> Result<Self, Error> {
        // Open the file specified in the configuration
        let file = File::open(&config.file_path).context("Failed to open file");
        let file = match file {
            Ok(file) => file,
            Err(err) => {
                return Err(err);
            }
        };

        // Create a buffered reader for efficient reading of the file
        let reader = BufReader::new(file);

        // Create a new schema instance based on the file schema specified in the configuration
        let schema = schema::Schema::new(&config.file_schema);
        let schema = match schema {
            Ok(schema) => schema,
            Err(err) => {
                return Err(err);
            }
        };

        // Get the newline characters defined in the schema
        let schema_line_newline_characters = schema.get_newline_characters();

        // Create a file buffer to handle reading and processing of lines
        let file_buffer = FileBuffer::new(reader, schema_line_newline_characters.to_owned());

        Ok(Self { config, schema, file_buffer })
    }

    /// Returns an iterator over the lines of the file.
    ///
    /// This method does not process the lines according to the schema.
    /// The schema attribute "lineseparator" is used for the line break.
    /// Use the `iter_mut` method to process each line according to the schema.
    ///
    /// # Returns
    ///
    /// An iterator that yields each line of the file as a `Result` containing either a `ReadLine` or an `std::io::Error`.
    pub fn lines(&mut self) -> impl Iterator<Item = Result<ReadLine, std::io::Error>> + '_ {
        std::iter::from_fn(move || self.file_buffer.next())
    }

    /// Returns an iterator that processes each line of the file according to the schema.
    ///
    /// # Returns
    ///
    /// An iterator that yields each processed line as a `Result` containing either a `ProcessedLineOk` or a `ProcessedLineError`.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = Result<ProcessedLineOk, ProcessedLineError>> + '_ {
        // Clone the schema to ensure each iteration uses a separate instance
        let schema = self.schema.clone();

        // Map each line of the file to a processed line based on the schema validation
        self.lines().map(move |result_read_line| {
            let read_line = match result_read_line {
                Ok(read_line) => read_line,
                Err(err) => {
                    return Err(ProcessedLineError { line_number: 0, message: format!("{:?}", err) });
                }
            };

            let result: Result<ProcessedLineOk, ProcessedLineError> =
                schema.validate_line(read_line.line_number, read_line.line_content.to_owned());
            match result {
                Ok(processed_line) => Ok(processed_line),
                Err(processed_line) => Err(processed_line),
            }
        })
    }
}

#[cfg(test)]
/// Module containing tests for the parser module.
mod tests {
    use crossbeam::channel::{unbounded, Receiver, Sender};
    use rayon::iter::{ParallelBridge, ParallelIterator};
    use std::thread;

    use super::*;

    /// Test function for the parser module.
    #[test]
    fn test_parser() {
        // Create a ParserConfig with file paths for data and schema files.
        let config = ParserConfig {
            file_path: "./example/fixedwidth_data.txt".to_string(),
            file_schema: "./example/fixedwidth_schema.xml".to_string(),
        };

        // Create a new Parser instance with the given config.
        let mut parser = Parser::new(config).unwrap();

        // Iterate over each line in the parser and process it.
        for line_result in parser.iter_mut() {
            match line_result {
                Ok(processed_line) => println!("{:?}", processed_line),
                Err(processed_line) => println!("Error processing line: {:?}", processed_line),
            }
        }
    }

    /// Test function for the parser module using multiple threads.
    #[test]
    fn test_parser_thread() {
        // Create a ParserConfig with file paths for data and schema files.
        let config = ParserConfig {
            file_path: "./example/fixedwidth_data.txt".to_string(),
            file_schema: "./example/fixedwidth_schema.xml".to_string(),
        };

        let n_workers = 4;

        // Create a new Parser instance with the given config.
        let mut parser = Parser::new(config).unwrap();

        // Define a type for line number and text.
        type LineNumberAndText = (usize, String);

        // Create a channel for sending and receiving line number and text.
        let (sender, receiver): (Sender<LineNumberAndText>, Receiver<LineNumberAndText>) = unbounded();
        let mut handles = vec![];

        // Spawn worker threads to process lines.
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

        // Iterate over each line in the parser and send it to the worker threads.
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

        // Collect the return errors from the worker threads.
        let mut return_errors: Vec<ProcessedLineError> = Vec::new();
        for handle in handles {
            let results = handle.join().expect("Failed to join thread");
            for result in results {
                return_errors.push(result);
            }
        }

        // Print the return errors, if any.
        if !return_errors.is_empty() {
            println!("Errors: {:?}", return_errors);
        }
    }

    /// Test function for the parser module using parallel iteration.
    #[test]
    fn test_parser_iter_par() {
        // Create a ParserConfig with file paths for data and schema files.
        let config = ParserConfig {
            file_path: "./example/fixedwidth_data.txt".to_string(),
            file_schema: "./example/fixedwidth_schema.xml".to_string(),
        };

        // Create a new Parser instance with the given config.
        let mut parser = Parser::new(config).unwrap();
        let schema = parser.schema.clone();

        // Iterate over each line in the parser in parallel and process it.
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
}
