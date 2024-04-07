use anyhow::Context;
use anyhow::{Error, Result};
use crossbeam::channel::{unbounded, Receiver, Sender};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::thread;

use chrono::naive::NaiveDate;

use crate::decimal_format;
use crate::schema::{self, Line};

type WorkerFunction = fn(Receiver<(usize, String)>, schema::Schema) -> Vec<Result<usize>>;

#[derive(Debug)]
pub struct ValidationError {
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

type LineNumberAndText = (usize, String);

impl Parser {
    pub fn new(config: ParserConfig) -> Self {
        Self { config }
    }

    pub fn start(&self) -> Result<(), Vec<(usize, ValidationError)>> {
        let (sender, receiver): (Sender<LineNumberAndText>, Receiver<LineNumberAndText>) =
            unbounded();
        let file = File::open(self.config.file_path.clone()).context("Failed to open file");
        let file = match file {
            Ok(file) => file,
            Err(err) => {
                return Err(vec![(0, ValidationError {
                    message: format!("Failed to open file: {}", err)
                })]);
            }
            
        };
        let reader = BufReader::new(file);

        let mut handles = vec![];
        for _ in 0..self.config.n_workers {
            let receiver = receiver.clone();
            let schema = schema::Schema::load(&self.config.file_schema).context("Failed to load schema");
            let schema = match schema {
                Ok(schema) => schema,
                Err(err) => {
                    return Err(vec![(0, ValidationError {
                        message: format!("Failed to load schema: {}", err)
                    })]);
                }
            };

            match self.config.fn_worker {
                Some(worker) => {
                    handles.push(thread::spawn(move || worker(receiver, schema)));
                }
                None => {
                    handles.push(thread::spawn(move || Self::worker(receiver, schema)));
                }
            }
        }
        let mut line_number = 1;
        for line_result in reader.lines() {
            let line_text = line_result.context("Failed to read line");
            let line_text = match line_text {
                Ok(line_text) => line_text,
                Err(err) => {
                    return Err(vec![(line_number, ValidationError {
                        message: format!("Failed to read line: {}", err)
                    })]);
                }
            };

            let result = sender
                .send((line_number, line_text))
                .context("Failed to send line to worker");
            match result {
                Ok(_) => {}
                Err(err) => {
                    return Err(vec![(line_number, ValidationError {
                        message: format!("Error processing line: {}", err)
                    })]); // TODO: Add optional if the first error should stop processing other lines. (ParserConfig)
                }
            }
            
            line_number += 1;
        }
        drop(sender);

        for handle in handles {
            let results = handle.join().unwrap();
            for result in results {
                match result {
                    Ok(line_number) => {
                        // TODO: Add line to the report as processed successfully.
                        println!("Line number {} processed successfully", line_number)
                    }
                    Err(err) => {
                        // TODO: Add line to the report as processed with errors.
                        println!("Error processing line: {}", err)
                    }
                }
            }
        }
        // TODO: Final report in the response format according to configuration (ParserConfig)
        Ok(())
    }

    fn worker(receiver: Receiver<LineNumberAndText>, schema: schema::Schema) -> Vec<Result<usize>> {
        let mut results: Vec<Result<usize, anyhow::Error>> = Vec::new();

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
                        results.push(Err(anyhow::anyhow!(
                            "No line type found for line number: {}",
                            line_number
                        )));
                        continue; // TODO: Add optional if the first error should stop processing other lines. (ParserConfig)
                    }
                };

                // Validate maxlength of the line
                if match_line_name.maxlength > 0 && line_text.len() != match_line_name.maxlength {
                    results.push(Err(anyhow::anyhow!(
                        "Line number {} has length {} but expected {}",
                        line_number, line_text.len(), match_line_name.maxlength
                    )));
                    continue; // TODO: Add optional if the first error should stop processing other lines. (ParserConfig)
                }

                // Validate each cell in the line
                let mut first_error: Option<Error> = None;
                for cell in match_line_name.cell {
                    match Self::validate_line(&cell, line_number, &line_text) {
                        Ok(_) => {}
                        Err(err) => {
                            first_error = Some(err);
                            break; // TODO: Add optional if the first error should stop processing other cells. (ParserConfig)
                        }
                    }
                }

                if first_error.is_some() {
                    results.push(Err(first_error.unwrap_or(Error::msg("Unknown error"))));
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
                match Self::validate_line(cell_line_condition, 0, line_text) {
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

    fn validate_line(cell: &schema::Cell, line_number: usize, line_text: &str) -> Result<()> {
        let cell_value: &str = &line_text[cell.start..cell.end];
    
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
                        return Err(anyhow::anyhow!(
                            "Invalid date format for line number: {}",
                            line_number
                        ));
                    }
                }
            } else if format.ctype == "string" {
                // Validate regex format in cell_value
                let re = regex::Regex::new(&format.pattern).unwrap();
                if re.is_match(cell_value) {
                    return Ok(());
                } else {
                    return Err(anyhow::anyhow!(
                        "Invalid regex format for line number: {}",
                        line_number
                    ));
                }
            } else if format.ctype == "number" {
                let formatter = decimal_format::DecimalFormat::new(&format.pattern).unwrap();
                formatter.validate_number(cell_value)
                    .map_err(|err| anyhow::anyhow!("{}: Invalid number format for line number: {}", line_number, err))?;
            }
        }
        Ok(())
    }
}