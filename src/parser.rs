use anyhow::Context;
use anyhow::Result;
use chrono::naive::NaiveDate;
use crossbeam::channel::{unbounded, Receiver, Sender};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::thread;

use crate::decimal_format;
use crate::schema::{self, Line};

pub type WorkerFunction =
    fn(Receiver<(usize, String)>, schema::Schema) -> Vec<Result<usize, ValidationError>>;
type LineNumberAndText = (usize, String);

#[derive(Debug)]
pub struct ValidationError {
    pub line: usize,
    pub message: String,
}
pub struct ParserConfig {
    pub file_path: String,
    pub file_schema: String,
    pub fn_worker: Option<WorkerFunction>,
    pub n_workers: usize,
    // TODO: add more configuration options. result_file_path, error_file_path, result_type, ...
}
pub struct Parser {
    config: ParserConfig,
}

struct LinesWithSeparator<R: BufRead> {
    reader: R,
    separator: Vec<u8>,
    buf: Vec<u8>,
    finished: bool,
}

impl<R: BufRead> LinesWithSeparator<R> {
    fn new(reader: R, separator: String) -> Self {
        Self {
            reader,
            separator: separator.into_bytes(),
            buf: Vec::new(),
            finished: false,
        }
    }
}

impl<R: BufRead> Iterator for LinesWithSeparator<R> {
    type Item = std::io::Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished && self.buf.is_empty() {
            return None;
        }

        let separator_str = match String::from_utf8(self.separator.clone()) {
            Ok(v) => v,
            Err(e) => return Some(Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e))),
        };

        let mut separator_bytes = Vec::new();
        let mut chars = separator_str.chars();
        while let Some(ch) = chars.next() {
            if ch == '\\' {
                match chars.next() {
                    Some('n') => separator_bytes.push(b'\n'),
                    Some('r') => separator_bytes.push(b'\r'),
                    Some('t') => separator_bytes.push(b'\t'),
                    Some('0') => separator_bytes.push(0),
                    Some(other) => separator_bytes.push(other as u8),
                    None => break,
                }
            } else {
                separator_bytes.push(ch as u8);
            }
        }

        let mut match_index = 0;

        loop {
            let mut byte = [0; 1];
            match self.reader.read_exact(&mut byte) {
                Ok(()) => {
                    self.buf.push(byte[0]);
                    if byte[0] == separator_bytes[match_index] {
                        match_index += 1;
                        if match_index == separator_bytes.len() {
                            self.buf.truncate(self.buf.len() - separator_bytes.len());
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
        self.buf.clear();
        Some(Ok(line))
    }
}

impl Parser {
    pub fn new(config: ParserConfig) -> Self {
        Self { config }
    }

    pub fn start(&self) -> Result<(), Vec<ValidationError>> {
        let (sender, receiver): (Sender<LineNumberAndText>, Receiver<LineNumberAndText>) =
            unbounded();
        let file = File::open(self.config.file_path.clone()).context("Failed to open file");
        let file = match file {
            Ok(file) => file,
            Err(err) => {
                return Err(vec![ValidationError {
                    line: 0,
                    message: format!("{}", err),
                }]);
            }
        };
        let reader = BufReader::new(file);

        let schema =
            schema::Schema::load(&self.config.file_schema).context("Failed to load schema");
        let schema = match schema {
            Ok(schema) => schema,
            Err(err) => {
                return Err(vec![ValidationError {
                    line: 0,
                    message: format!("{}", err),
                }]);
            }
        };

        let schema_line_separator = schema.get_line_separator();

        let mut handles = vec![];
        for _ in 0..self.config.n_workers {
            let receiver = receiver.clone();
            let schema = schema.clone();

            match self.config.fn_worker {
                Some(worker) => {
                    handles.push(thread::spawn(move || worker(receiver, schema)));
                }
                None => {
                    handles.push(thread::spawn(move || Self::worker(receiver, schema)));
                }
            }
        }

        let lines: LinesWithSeparator<BufReader<File>> =
            LinesWithSeparator::new(reader, schema_line_separator);

        let mut line_number = 1;
        for line_result in lines {
            let line_text = line_result.context("Failed to read line");
            let line_text = match line_text {
                Ok(line_text) => line_text,
                Err(err) => {
                    return Err(vec![ValidationError {
                        line: line_number,
                        message: format!("{}", err),
                    }]);
                }
            };

            let result = sender
                .send((line_number, line_text))
                .context("Failed to send line to worker");
            match result {
                Ok(_) => {}
                Err(err) => {
                    return Err(vec![ValidationError {
                        line: line_number,
                        message: format!("{}", err),
                    }]); // TODO: Add optional if the first error should stop processing other lines. (ParserConfig)
                }
            }

            line_number += 1;
        }
        drop(sender);

        let mut return_errors: Vec<ValidationError> = Vec::new();
        for handle in handles {
            let results = handle.join().unwrap();
            for result in results {
                match result {
                    Ok(_line_number) => {
                        // TODO: Add line to the report as processed successfully.
                    }
                    Err(v) => {
                        return_errors.push(ValidationError {
                            line: v.line,
                            message: v.message,
                        });
                    }
                }
            }
        }

        if !return_errors.is_empty() {
            return Err(return_errors);
        }
        // TODO: Final report in the response format according to configuration (ParserConfig)
        Ok(())
    }

    fn worker(
        receiver: Receiver<LineNumberAndText>,
        schema: schema::Schema,
    ) -> Vec<Result<usize, ValidationError>> {
        let mut results: Vec<Result<usize, ValidationError>> = Vec::new();

        let schema_lines_with_condition: Vec<(String, Vec<schema::Cell>)> =
            schema.get_line_conditions().to_owned();

        for (line_number, line_text) in receiver {
            if schema.get_schema_type() == "fixedwidthschema" {
                // Find the line type that matches the line condition (from schema)
                let match_line_name: Option<Line> = Self::find_matching_schema_line_type(
                    &line_text,
                    &schema_lines_with_condition,
                    schema.clone(),
                );

                let match_line_name: Line = match match_line_name {
                    Some(line) => line,
                    None => {
                        results.push(Err(ValidationError {
                            line: line_number,
                            message: "[err:001]|line|no match found for schema line type"
                                .to_string(),
                        }));
                        continue; // TODO: Add optional if the first error should stop processing other lines. (ParserConfig)
                    }
                };

                // Validate maxlength of the line
                if match_line_name.maxlength > 0 && line_text.len() != match_line_name.maxlength {
                    results.push(Err(ValidationError {
                        line: line_number,
                        message: format!(
                            "[err:002]|line|maxlength|the line has length {} but was expected {}",
                            line_text.len(),
                            match_line_name.maxlength
                        ),
                    }));
                    continue; // TODO: Add optional if the first error should stop processing other lines. (ParserConfig)
                }

                // Validate each cell in the line
                let mut first_error: Option<String> = None;
                for cell in match_line_name.cell {
                    match Self::validate_line(&cell, &line_text) {
                        Ok(_) => {}
                        Err(err) => {
                            first_error = err.to_string().into();
                            break; // TODO: Add optional if the first error should stop processing other cells. (ParserConfig)
                        }
                    }
                }

                if first_error.is_some() {
                    results.push(Err(ValidationError {
                        line: line_number,
                        message: first_error.unwrap_or("Unknown error".to_string()),
                    }));
                    continue; // TODO: Add optional if the first error should stop processing other lines. (ParserConfig)
                }
                results.push(Ok(line_number));
            } else if schema.get_schema_type() == "delimitedschema" {
                todo!("Delimited schema not implemented yet");
            } else if schema.get_schema_type() == "csvschema" {
                todo!("CSV schema not implemented yet");
            }
        }

        results
    }

    pub fn find_matching_schema_line_type(
        line_text: &str,
        schema_lines_with_condition: &Vec<(String, Vec<schema::Cell>)>,
        schema: schema::Schema,
    ) -> Option<Line> {
        let mut match_line_name = "";
        for (line_name, cell_conditions) in schema_lines_with_condition {
            let mut line_condition_met = false;
            for cell_line_condition in cell_conditions {
                let cell_value: &str =
                    &line_text[cell_line_condition.start..cell_line_condition.end];

                /*
                Validate the cell value previously to check the line condition
                When there is a <format> together with a <linecondition>

                    <cell name="Foo" length="5">
                        <format type="regex" pattern=".*"/>
                        <linecondition><match type="string" pattern="H"/></linecondition>
                    </cell>
                */
                match Self::validate_line(cell_line_condition, line_text) {
                    Ok(_) => {}
                    Err(_) => {
                        continue;
                    }
                }

                // Check if the line condition is met
                // TODO: Add support for other linecondition types (e.g. regex, number, ...)
                if cell_line_condition.linecondition_type.is_none()
                    || cell_line_condition.linecondition_type == Some("string".to_string())
                {
                    line_condition_met =
                        cell_value == cell_line_condition.linecondition_pattern.as_ref().unwrap();
                } else {
                    todo!("Line condition type not implemented yet");
                }
            }
            if line_condition_met {
                match_line_name = line_name;
                break;
            }
        }

        if match_line_name.is_empty() {
            // Get only the first line without conditions.
            // If there is more than one line without conditions, in that case it should return None.
            return schema.get_first_line_without_condition();
        }

        schema.get_line_by_linetype(match_line_name)
    }

    fn validate_line(cell: &schema::Cell, line_text: &str) -> Result<(), String> {
        let cell_name = &cell.name;
        let cell_value: Option<&str> = line_text.get(cell.start..cell.end);
        let cell_value = match cell_value {
            Some(cell_value) => cell_value,
            None => {
                return Err(format!(
                    "[err:003]|{}|invalid range [{}]-[{}]",
                    cell_name, cell.start, cell.end
                ));
            }
        };
        if let Some(format) = &cell.format {
            // TODO: add more validation for other format types (e.g. number, regex, ...)
            if format.ctype == "date" {
                // validate date format in cell_value
                let dt = NaiveDate::parse_from_str(cell_value, &format.pattern);
                match dt {
                    Ok(_) => {
                        return Ok(());
                    }
                    Err(_) => {
                        return Err(format!(
                            "[err:004]|{}|{}|pattern:{}",
                            cell_name, format.ctype, format.pattern
                        ));
                    }
                }
            } else if format.ctype == "string" {
                // Validate regex format in cell_value
                let re = regex::Regex::new(&format.pattern).unwrap();
                if re.is_match(cell_value) {
                    return Ok(());
                } else {
                    return Err(format!(
                        "[err:005]|{}|{}|pattern:{}",
                        cell_name, format.ctype, format.pattern
                    ));
                }
            } else if format.ctype == "number" {
                let formatter = decimal_format::DecimalFormat::new(&format.pattern).unwrap();
                match formatter.validate_number(cell_value) {
                    Ok(_) => {
                        return Ok(());
                    }
                    Err(_) => {
                        return Err(format!(
                            "[err:006]|{}|{}|pattern:{}",
                            cell_name, format.ctype, format.pattern
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}
