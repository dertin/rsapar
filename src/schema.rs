use anyhow::{anyhow, Context, Error, Result};
use chrono::NaiveDate;
use std::{collections::{HashMap, HashSet}, fs::File, io::BufReader};
use xml::reader::{EventReader, XmlEvent};

use crate::{decimal_format, ProcessedLineError, ProcessedLineOk};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Format {
    pub ctype: String,
    pub pattern: String,
}
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct LineCondition {
    pub matchpattern: String,
}
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Cell {
    pub name: String,
    pub length: usize,
    pub start: usize,
    pub end: usize,
    pub format: Option<Format>,
    pub linecondition_type: Option<String>,
    pub linecondition_pattern: Option<String>,
    pub alignment: String,
    pub padcharacter: String,
}

#[derive(Debug, Clone)]
pub struct Line {
    pub linetype: String,
    pub maxlength: usize,
    pub occurs: String,
    pub cell: Vec<Cell>,
    pub minlength: usize,
    pub padcharacter: String,
}

#[derive(Clone, Debug)]
pub struct FixedWidthSchema {
    pub lineseparator: String,
    pub lines: Vec<Line>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct CsvSchema {
    pub lines: Vec<Line>, // TODO: implement CSV schema
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct Schema {
    pub fixedwidthschema: Option<FixedWidthSchema>,
    pub csvschema: Option<CsvSchema>, // TODO: implement CSV schema
}

impl Schema {
    pub fn new(path: &str) -> Result<Self, Error> {
        let file = File::open(path)?;
        let file = BufReader::new(file);
        let parser = EventReader::new(file);

        let mut schema = Schema {
            fixedwidthschema: Some(FixedWidthSchema { lineseparator: "\n".to_string(), lines: vec![] }),
            csvschema: None,
        };

        let mut temp_line = Line {
            linetype: String::new(),
            maxlength: 0,
            occurs: String::new(),
            cell: vec![],
            minlength: 0,
            padcharacter: String::from(" "),
        };
        let mut in_line = false;
        let mut in_cell = false;
        let mut temp_format: Option<Format> = None;
        let mut end_cell = 0;

        let mut seen_linetypes = HashSet::new();

        for e in parser {
            match e {
                Ok(XmlEvent::StartElement { name, attributes, .. }) => match name.local_name.as_str() {
                    "fixedwidthschema" => {
                        for attr in attributes {
                            if attr.name.local_name == "lineseparator" {
                                if let Some(fixed_width_schema) = &mut schema.fixedwidthschema {
                                    fixed_width_schema.lineseparator = attr.value;
                                }
                            }
                        }
                    }
                    "line" => {
                        in_line = true;
                        temp_line = Line {
                            linetype: String::new(),
                            maxlength: 0,
                            occurs: String::new(),
                            cell: vec![],
                            minlength: 0,
                            padcharacter: String::from(" "),
                        };
                        for attr in attributes {
                            match attr.name.local_name.as_str() {
                                "linetype" => {
                                    if seen_linetypes.contains(&attr.value) {
                                        return Err(anyhow!("Duplicate linetype: {}", attr.value));
                                    }
                                    seen_linetypes.insert(attr.value.clone());
                                    temp_line.linetype = attr.value;
                                }
                                "occurs" => temp_line.occurs = attr.value, // TODO: not used by the parser yet.
                                "maxlength" => temp_line.maxlength = attr.value.parse().unwrap_or(0),
                                "minlength" => {
                                    // TODO: not used by the parser yet.
                                    temp_line.minlength = attr.value.parse().unwrap_or(0)
                                }
                                "padcharacter" => temp_line.padcharacter = attr.value,
                                _ => (),
                            }
                        }
                    }
                    "cell" if in_line => {
                        in_cell = true;
                        let mut cell_name = String::new();
                        let mut cell_length = 0;
                        let mut cell_alignment = String::new();
                        let mut cell_padcharacter = temp_line.padcharacter.to_owned();

                        for attr in attributes {
                            match attr.name.local_name.as_str() {
                                "name" => cell_name = attr.value,
                                "length" => cell_length = attr.value.parse().unwrap_or(0),
                                "alignment" => cell_alignment = attr.value,
                                "padcharacter" => cell_padcharacter = attr.value,
                                _ => (),
                            }
                        }

                        end_cell += cell_length;

                        temp_line.cell.push(Cell {
                            name: cell_name,
                            length: cell_length,
                            start: end_cell - cell_length,
                            end: end_cell,
                            format: None,
                            linecondition_type: None,
                            linecondition_pattern: None,
                            alignment: cell_alignment,
                            padcharacter: cell_padcharacter,
                        });
                    }
                    "format" if in_cell => {
                        let mut ctype = String::new();
                        let mut pattern = String::new();
                        for attr in attributes {
                            match attr.name.local_name.as_str() {
                                "type" => ctype = attr.value.to_lowercase(),
                                "pattern" => pattern = attr.value,
                                _ => (),
                            }
                        }
                        temp_format = Some(Format { ctype, pattern });
                    }
                    "match" if in_cell => {
                        let mut matchtype = String::new();
                        let mut matchpattern = String::new();

                        for attr in attributes {
                            if attr.name.local_name == "type" {
                                matchtype = attr.value.to_lowercase();
                            }
                            if attr.name.local_name == "pattern" {
                                matchpattern = attr.value;
                            }
                        }
                        if let Some(cell) = temp_line.cell.last_mut() {
                            cell.linecondition_type = Some(matchtype);
                            cell.linecondition_pattern = Some(matchpattern);
                        }
                    }
                    _ => (),
                },
                Ok(XmlEvent::EndElement { name, .. }) => match name.local_name.as_str() {
                    "cell" => {
                        if in_cell {
                            if let Some(cell) = temp_line.cell.last_mut() {
                                cell.format = temp_format.take();
                            }
                            in_cell = false;
                        }
                    }
                    "line" => {
                        if in_line {
                            if let Some(fixed_width_schema) = &mut schema.fixedwidthschema {
                                fixed_width_schema.lines.push(temp_line.to_owned());
                            }

                            in_line = false;
                            end_cell = 0;
                        }
                    }
                    _ => (),
                },
                Err(e) => return Err(e).context("Error parsing XML"),
                _ => (),
            }
        }

        Ok(schema)
    }

    pub fn get_line_conditions(&self) -> Vec<(String, std::vec::Vec<Cell>)> {
        let binding = self.get_binding();
        binding
            .lines
            .iter()
            .filter_map(|line| {
                let cells_with_condition: Vec<_> =
                    line.cell.iter().filter(|cell| cell.linecondition_pattern.is_some()).cloned().collect();
                if cells_with_condition.is_empty() {
                    None
                } else {
                    Some((line.linetype.to_owned(), cells_with_condition))
                }
            })
            .collect()
    }

    pub fn get_first_line_without_condition(&self) -> Option<Line> {
        let binding = self.get_binding();
        let lines_without_condition: Vec<_> = binding
            .lines
            .iter()
            .filter(|line| line.cell.iter().all(|cell| cell.linecondition_pattern.is_none()))
            .cloned()
            .collect();

        if lines_without_condition.len() > 1 {
            // If there is more than one line without conditions, in that case it should return None.
            return None;
        }

        lines_without_condition.first().cloned()
    }

    pub fn get_schema_type(&self) -> &str {
        if self.fixedwidthschema.is_some() {
            "fixedwidthschema"
        } else {
            // TODO: implement more schemes
            "csvschema"
            // delimitedschema, jsonschema, ...
        }
    }

    pub fn get_line_by_linetype(&self, linetype: &str) -> Option<Line> {
        let binding = self.get_binding();
        let line = binding.lines.iter().find(|line| line.linetype == linetype).cloned();
        line
    }

    pub fn get_newline_characters(&self) -> String {
        let binding = self.get_binding();
        binding.lineseparator
    }

    fn get_binding(&self) -> FixedWidthSchema {
        // For now it is only implemented for fixed width scheme.
        // TODO: implement for CSV schema (should be equal to fixed width)
        let fixedwidthschema: &Option<FixedWidthSchema> = &self.fixedwidthschema; // hardcoded to fixed width
        let binding: FixedWidthSchema = match fixedwidthschema {
            Some(fixedwidthschema) => fixedwidthschema.to_owned(),
            None => todo!("invalid schema"),
        };
        binding
    }

    pub fn find_matching_schema_line_type(
        &self, line_text: &str, schema_lines_with_condition: &Vec<(String, Vec<Cell>)>,
    ) -> Option<Line> {
        let mut match_line_name = "";
        for (line_name, cell_conditions) in schema_lines_with_condition {
            let mut line_condition_met = false;
            for cell_line_condition in cell_conditions {
                let cell_value: &str = &line_text[cell_line_condition.start..cell_line_condition.end];

                /*
                Validate the cell value previously to check the line condition
                When there is a <format> together with a <linecondition>

                    <cell name="Foo" length="5">
                        <format type="regex" pattern=".*"/>
                        <linecondition><match type="string" pattern="H"/></linecondition>
                    </cell>
                */
                match Self::validate_cell(cell_line_condition, line_text) {
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
                    line_condition_met = cell_value == cell_line_condition.linecondition_pattern.as_ref().unwrap();
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
            return self.get_first_line_without_condition();
        }

        self.get_line_by_linetype(match_line_name)
    }

    pub fn validate_line(&self, line_number: usize, line_text: String) -> Result<ProcessedLineOk, ProcessedLineError> {
        let schema_lines_with_condition: Vec<(String, Vec<Cell>)> = self.get_line_conditions().to_owned();

        if self.get_schema_type() == "fixedwidthschema" {
            // Find the line type that matches the line condition (from schema)
            let match_line_name: Option<Line> =
                self.find_matching_schema_line_type(&line_text, &schema_lines_with_condition);

            let match_line_name: Line = match match_line_name {
                Some(line) => line,
                None => {
                    return Err(ProcessedLineError {
                        line_number,
                        message: "[err:001]|line|no match found for schema line type".to_string(),
                    });
                    // TODO: Add optional if the first error should stop processing other lines. (ParserConfig)
                }
            };

            // Validate maxlength of the line
            if match_line_name.maxlength > 0 && line_text.len() != match_line_name.maxlength {
                return Err(ProcessedLineError {
                    line_number,
                    message: format!(
                        "[err:002]|line|maxlength|the line has length {} but was expected {}",
                        line_text.len(),
                        match_line_name.maxlength
                    ),
                });
                // TODO: Add optional if the first error should stop processing other lines. (ParserConfig)
            }

            // Validate each cell in the line
            let mut cell_values: HashMap<String, String> = Default::default();

            let mut first_error: Option<String> = None;
            for cell in match_line_name.cell {
                match Self::validate_cell(&cell, &line_text) {
                    Ok(cell_value) => {
                        cell_values.insert(cell.name, cell_value);
                    }
                    Err(err) => {
                        first_error = err.to_string().into();
                        break; // TODO: Add optional if the first error should stop processing other cells. (ParserConfig)
                    }
                }
            }

            if first_error.is_some() {
                return Err(ProcessedLineError { line_number, message: first_error.unwrap_or("Unknown error".to_string()) });
                // TODO: Add optional if the first error should stop processing other lines. (ParserConfig)
            }

            Ok(ProcessedLineOk { line_number, cell_values})
        } else if self.get_schema_type() == "delimitedschema" {
            todo!("Delimited schema not implemented yet");
        } else if self.get_schema_type() == "csvschema" {
            todo!("CSV schema not implemented yet");
        } else {
            todo!("Schema type not implemented yet");
        }
    }

    fn validate_cell(cell: &Cell, line_text: &str) -> Result<String, String> {
        let cell_name = &cell.name;
        let mut cell_alignment = cell.alignment.to_owned();
        let cell_padcharacter = &cell.padcharacter;

        let cell_value: Option<&str> = line_text.get(cell.start..cell.end);
        let cell_value = match cell_value {
            Some(cell_value) => cell_value,
            None => {
                return Err(format!("[err:003]|{}|invalid range [{}]-[{}]", cell_name, cell.start, cell.end));
            }
        };
        if let Some(format) = &cell.format {
            if cell_alignment.is_empty() && format.ctype == "number" {
                cell_alignment = "right".to_string();
            } else if cell_alignment.is_empty() {
                cell_alignment = "left".to_string();
            }

            let cell_value = match cell_alignment.as_str() {
                "right" => {
                    let cell_padcharacter_vec: Vec<char> = cell_padcharacter.chars().collect();
                    let cell_padcharacter_slice: &[char] = &cell_padcharacter_vec;
                    cell_value.trim_start_matches(cell_padcharacter_slice)
                }
                "left" => {
                    let cell_padcharacter_vec: Vec<char> = cell_padcharacter.chars().collect();
                    let cell_padcharacter_slice: &[char] = &cell_padcharacter_vec;
                    cell_value.trim_end_matches(cell_padcharacter_slice)
                }
                "center" => {
                    let cell_padcharacter_vec: Vec<char> = cell_padcharacter.chars().collect();
                    let cell_padcharacter_slice: &[char] = &cell_padcharacter_vec;
                    cell_value.trim_matches(cell_padcharacter_slice)
                }
                _ => cell_value,
            };

            // TODO: add more validation for other format types (e.g. number, regex, ...)
            if format.ctype == "date" {
                // validate date format in cell_value
                let dt = NaiveDate::parse_from_str(cell_value, &format.pattern);
                match dt {
                    Ok(_) => {
                        return Ok(cell_value.to_string());
                    }
                    Err(_) => {
                        return Err(format!("[err:004]|{}|{}|pattern:{}", cell_name, format.ctype, format.pattern));
                    }
                }
            } else if format.ctype == "string" {
                // Validate regex format in cell_value
                let re = regex::Regex::new(&format.pattern).unwrap();
                if re.is_match(cell_value) {
                    return Ok(cell_value.to_string());
                } else {
                    return Err(format!("[err:005]|{}|{}|pattern:{}", cell_name, format.ctype, format.pattern));
                }
            } else if format.ctype == "number" {
                let formatter = decimal_format::DecimalFormat::new(&format.pattern).unwrap();
                match formatter.validate_number(cell_value) {
                    Ok(_) => {
                        return Ok(cell_value.to_string());
                    }
                    Err(_) => {
                        return Err(format!("[err:006]|{}|{}|pattern:{}", cell_name, format.ctype, format.pattern));
                    }
                }
            }
        }
        Ok(cell_value.to_string())
    }
}
