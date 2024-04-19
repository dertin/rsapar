use anyhow::Context;
use anyhow::Error;
use anyhow::Result;

use crossbeam::channel::Receiver;
use std::fs::File;
use std::io::{BufRead, BufReader};

use crate::schema;

pub type WorkerFunction = fn(Receiver<(usize, String)>, schema::Schema) -> Vec<Result<ProcessedLine, ProcessedLine>>;

#[derive(Debug)]
pub struct ProcessedLine {
    pub line_number: usize,
    pub message: String,
}
#[derive(Debug)]
pub struct ReadLine {
    pub line_number: usize,
    pub line_content: String,
}

#[derive(Clone)]
pub struct ParserConfig {
    pub file_path: String,
    pub file_schema: String,
    // TODO: add more configuration options. result_file_path, error_file_path, result_type, ...
}

struct FileBuffer<R: BufRead> {
    reader: R,
    current_line: usize,
    newline_characters: Vec<u8>,
    buf: Vec<u8>,
    finished: bool,
}

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

    pub fn iter_mut(&mut self) -> impl Iterator<Item = Result<ProcessedLine, ProcessedLine>> + '_ {
        let schema = self.schema.clone();

        self.lines().map(move |result_read_line| {
            let read_line = match result_read_line {
                Ok(read_line) => read_line,
                Err(err) => {
                    return Err(ProcessedLine { line_number: 0, message: format!("{:?}", err) });
                }
            };

            let result: Result<ProcessedLine, ProcessedLine> =
                schema.validate_line(read_line.line_number, read_line.line_content.to_owned());
            match result {
                Ok(processed_line) => Ok(processed_line),
                Err(processed_line) => Err(processed_line),
            }
        })
    }
}
