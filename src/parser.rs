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

#[derive(Clone, Debug)]
pub struct ParserConfig {
    pub file_path: String,
    pub file_schema: String,
}
#[derive(Debug)]
struct FileBuffer<R: BufRead> {
    reader: R,
    current_line: usize,
    newline_characters: Vec<u8>,
    buf: Vec<u8>,
    finished: bool,
}

#[derive(Debug)]
pub struct Parser {
    pub config: ParserConfig,
    pub schema: schema::Schema,
    file_buffer: FileBuffer<BufReader<File>>,
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

impl Parser {
    pub fn new(config: ParserConfig) -> Result<Self, Error> {
        let file = File::open(&config.file_path).context("Failed to open file");
        let file = match file {
            Ok(file) => file,
            Err(err) => {
                return Err(err);
            }
        };
        let reader = BufReader::new(file);

        let schema = schema::Schema::new(&config.file_schema);
        let schema = match schema {
            Ok(schema) => schema,
            Err(err) => {
                return Err(err);
            }
        };

        let schema_line_newline_characters = schema.get_newline_characters();

        let file_buffer = FileBuffer::new(reader, schema_line_newline_characters);

        Ok(Self { config, schema, file_buffer })
    }

    pub fn lines(&mut self) -> impl Iterator<Item = Result<ReadLine, std::io::Error>> + '_ {
        std::iter::from_fn(move || self.file_buffer.next())
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = Result<ProcessedLineOk, ProcessedLineError>> + '_ {
        let schema = self.schema.clone();

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
mod tests {
    use std::thread;
    use crossbeam::channel::{unbounded, Receiver, Sender};
    use rayon::iter::{ParallelBridge, ParallelIterator};

    use super::*;

    #[test]
    fn test_parser() {
        let config = ParserConfig {
            file_path: "./example/fixedwidth_data.txt".to_string(),
            file_schema: "./example/fixedwidth_schema.xml".to_string(),
        };

        let mut parser = Parser::new(config).unwrap();

        for line_result in parser.iter_mut() {
            match line_result {
                Ok(processed_line) => println!("{:?}", processed_line),
                Err(processed_line) => println!("Error processing line: {:?}", processed_line),
            }
        }
    }

    #[test]
    fn test_parser_thread() {
        let config = ParserConfig {
            file_path: "./example/fixedwidth_data.txt".to_string(),
            file_schema: "./example/fixedwidth_schema.xml".to_string(),
        };

        let n_workers = 4;

        let mut parser = Parser::new(config).unwrap();

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
        let config = ParserConfig {
            file_path: "./example/fixedwidth_data.txt".to_string(),
            file_schema: "./example/fixedwidth_schema.xml".to_string(),
        };

        let mut parser = Parser::new(config).unwrap();
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

}
