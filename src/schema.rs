use anyhow::{anyhow, Context, Error, Result};
use chrono::NaiveDate;
use indexmap::map::IndexMap;
use std::{collections::HashSet, fs::File, io::BufReader};
use xml::reader::{EventReader, XmlEvent};

use crate::{decimal_format, ProcessedLineError, ProcessedLineOk};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Format {
    pub ctype: String,
    pub pattern: String,
    pub regex_pattern: Option<regex::Regex>,
}
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct LineCondition {
    pub matchpattern: String,
}

#[derive(Debug, Clone, Default)]
pub struct Line {
    pub linetype: String,
    pub maxlength: usize,
    pub occurs: String,
    pub cell: Vec<Cell>,
    pub minlength: usize,
    pub padcharacter: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
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

#[derive(Clone, Debug, Default)]
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
    /// Load schema from XML file
    pub fn new(path: &str) -> Result<Self, Error> {
        let file = File::open(path)?;
        let file = BufReader::new(file);
        let parser = EventReader::new(file);

        let mut schema = Schema { fixedwidthschema: None, csvschema: None };

        let mut temp_line = Line { padcharacter: String::from(" "), ..Default::default() };
        let mut in_line = false;
        let mut in_cell = false;
        let mut temp_format: Option<Format> = None;
        let mut end_cell = 0;

        let mut seen_linetypes = HashSet::new();

        for e in parser {
            match e {
                Ok(XmlEvent::StartElement { name, attributes, .. }) => match name.local_name.as_str() {
                    "fixedwidthschema" => {
                        schema.fixedwidthschema =
                            Some(FixedWidthSchema { lineseparator: "\n".to_string(), ..Default::default() });
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
                        temp_line = Line { padcharacter: String::from(" "), ..Default::default() };
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

                        let mut temp_cell =
                            Cell { padcharacter: temp_line.padcharacter.to_owned(), ..Default::default() };

                        for attr in attributes {
                            match attr.name.local_name.as_str() {
                                "name" => temp_cell.name = attr.value,
                                "length" => temp_cell.length = attr.value.parse().unwrap_or(0),
                                "alignment" => temp_cell.alignment = attr.value,
                                "padcharacter" => temp_cell.padcharacter = attr.value,
                                _ => (),
                            }
                        }

                        end_cell += temp_cell.length;

                        temp_line.cell.push(Cell {
                            name: temp_cell.name,
                            length: temp_cell.length,
                            start: end_cell - temp_cell.length,
                            end: end_cell,
                            alignment: temp_cell.alignment,
                            padcharacter: temp_cell.padcharacter,
                            ..Default::default()
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

                        let mut regex_pattern = None;
                        if ctype == "string" {
                            regex_pattern = match regex::Regex::new(&pattern) {
                                Ok(re) => Some(re),
                                Err(e) => {
                                    return Err(e).context(format!("Error compiling regex pattern: {}", pattern));
                                }
                            };
                        }
                        
                        temp_format = Some(Format { ctype, pattern, regex_pattern});
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

    /// Get all line conditions from the schema
    /// Returns a vector of tuples with the line type and the cells with conditions
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

    /// Get the first line without conditions
    /// Returns a tuple with the line type and the line without conditions or None if there is more than one line without conditions
    pub fn get_first_line_without_condition(&self) -> Option<(String, Line)> {
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

        match lines_without_condition.first().cloned() {
            Some(line) => Some((line.linetype.to_owned(), line)),
            None => None,
        }
    }

    /// Get the schema type
    pub fn get_schema_type(&self) -> &str {
        if self.fixedwidthschema.is_some() {
            "fixedwidthschema"
        } else {
            // TODO: implement for CSV schema
            // "csvschema"
            todo!("Schema csvschema not implemented yet");
        }
    }

    /// Get the line by linetype
    /// Returns the line or None if the linetype is not found
    pub fn get_line_by_linetype(&self, linetype: &str) -> Option<Line> {
        let binding = self.get_binding();
        let line = binding.lines.iter().find(|line| line.linetype == linetype).cloned();
        line
    }

    /// Get the newline characters
    /// Example: "\n", "\r\n", ...
    pub fn get_newline_characters(&self) -> &str {
        let binding = self.get_binding();
        &binding.lineseparator
    }

    /// Get binding schema (fixed width or csv)
    fn get_binding(&self) -> &FixedWidthSchema {
        // For now it is only implemented for fixed width scheme.
        match self.fixedwidthschema.as_ref() {
            Some(fixed_width_schema) => fixed_width_schema,
            None => {
                // TODO: implement for CSV schema (should be equal to fixed width)
                panic!("Schema not implemented yet");
            }
        }
    }

    /// Find the line type that matches the line condition
    pub fn find_matching_schema_linetype(
        &self, line_text: &str, schema_lines_with_condition: &Vec<(String, Vec<Cell>)>,
    ) -> Option<(String, Line)> {
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

        let line: Option<Line> = self.get_line_by_linetype(match_line_name);

        line.map(|line| (match_line_name.to_owned(), line))
    }

    /// Compiled regex for line condition
    pub fn compile_line_condition(&self, line_condition: &LineCondition) -> regex::Regex {
        regex::Regex::new(&line_condition.matchpattern).unwrap()
    }

    /// Compiled regex for cell format
    pub fn compile_cell_format(&self, format: &Format) -> regex::Regex {
        regex::Regex::new(&format.pattern).unwrap()
    }

    /// Validate a line
    /// Returns:
    /// - crate::parser::ProcessedLineOk    -> if the line is valid
    /// - crate::parser::ProcessedLineError -> if the line is invalid
    ///
    /// The ProcessedLineOk contains:
    /// - line_number: the line number
    /// - cell_values: the cell values
    /// - linetype: the line type
    ///
    /// The ProcessedLineError contains:
    /// - line_number: the line number
    /// - message: the error message
    ///
    /// The error message format is:
    /// [err:xxx]|line|message -> for line errors
    /// [err:xxx]|cellname|ctype|message -> for cell errors
    ///
    pub fn validate_line(&self, line_number: usize, line_text: String) -> Result<ProcessedLineOk, ProcessedLineError> {
        let schema_lines_with_condition: Vec<(String, Vec<Cell>)> = self.get_line_conditions().to_owned();

        if self.get_schema_type() == "fixedwidthschema" {
            // Find the line type that matches the line condition (from schema)
            let match_line: Option<(String, Line)> =
                self.find_matching_schema_linetype(&line_text, &schema_lines_with_condition);

            let (linetype, match_line) = match match_line {
                Some(match_line) => (match_line.0, match_line.1),
                None => {
                    return Err(ProcessedLineError {
                        line_number,
                        message: "[err:001]|line|no match found for schema line type".to_string(),
                    });
                    // TODO: Add optional if the first error should stop processing other lines. (ParserConfig)
                }
            };

            // Validate maxlength of the line
            if match_line.maxlength > 0 && line_text.len() != match_line.maxlength {
                return Err(ProcessedLineError {
                    line_number,
                    message: format!(
                        "[err:002]|line|maxlength|the line has length {} but was expected {}",
                        line_text.len(),
                        match_line.maxlength
                    ),
                });
                // TODO: Add optional if the first error should stop processing other lines. (ParserConfig)
            }

            // Validate each cell in the line
            let mut cell_values: IndexMap<String, String> = Default::default();

            let mut first_error: Option<String> = None;
            for cell in match_line.cell {
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
                return Err(ProcessedLineError {
                    line_number,
                    message: first_error.unwrap_or("Unknown error".to_string()),
                });
                // TODO: Add optional if the first error should stop processing other lines. (ParserConfig)
            }

            Ok(ProcessedLineOk { line_number, cell_values, linetype })
        } else if self.get_schema_type() == "csvschema" {
            todo!("CSV schema not implemented yet");
        } else {
            todo!("Schema type not implemented yet");
        }
    }

    /// Validate a cell
    /// Returns:
    /// - Ok(cell_value) 'cell_value' as String
    /// - Err(message)
    ///
    fn validate_cell(cell: &Cell, line_text: &str) -> Result<String, String> {
        let cell_name = &cell.name;
        let mut cell_alignment = cell.alignment.to_owned();
        let cell_padcharacter = &cell.padcharacter;

        let cell_value: Option<&str> = line_text.get(cell.start..cell.end);
        let cell_value = match cell_value {
            Some(cell_value) => cell_value,
            None => {
                return Err(format!("[err:003]|{}|range|invalid [{}]-[{}]", cell_name, cell.start, cell.end));
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
                        return Err(format!("[err:004]|{}|{}|pattern:[{}]", cell_name, format.ctype, format.pattern));
                    }
                }
            } else if format.ctype == "string" {
                // Validate regex format in cell_value
                if let Some(re) = &format.regex_pattern {
                    if re.is_match(cell_value) {
                        return Ok(cell_value.to_string());
                    } else {
                        return Err(format!("[err:005]|{}|{}|pattern:[{}]", cell_name, format.ctype, format.pattern));
                    }
                } else {
                    return Err(format!("[err:006]|{}|{}|pattern:[{}]", cell_name, format.ctype, format.pattern));
                }
                
            } else if format.ctype == "number" {
                let formatter = decimal_format::DecimalFormat::new(&format.pattern).unwrap();
                match formatter.validate_number(cell_value) {
                    Ok(_) => {
                        return Ok(cell_value.to_string());
                    }
                    Err(_) => {
                        return Err(format!("[err:007]|{}|{}|pattern:[{}]", cell_name, format.ctype, format.pattern));
                    }
                }
            }
        }
        Ok(cell_value.to_string())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_fixedwidthschema() {
        let schema: Schema = Schema::new("./example/fixedwidth_schema.xml").expect("Failed to load schema");
        assert!(schema.fixedwidthschema.is_some());
    }
}
