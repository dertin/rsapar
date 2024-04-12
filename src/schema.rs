use anyhow::Context;
use anyhow::Error;
use anyhow::Result;
use anyhow::anyhow;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use xml::reader::{EventReader, XmlEvent};

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
    pub fn load(path: &str) -> Result<Schema, Error> {
        let file = File::open(path)?;
        let file = BufReader::new(file);
        let parser = EventReader::new(file);

        let mut schema = Schema {
            fixedwidthschema: Some(FixedWidthSchema {
                lineseparator: "\n".to_string(),
                lines: vec![],
            }),
            csvschema: None,
        };

        let mut temp_line = Line {
            linetype: String::new(),
            maxlength: 0,
            occurs: String::new(),
            cell: vec![],
            minlength: 0,
            padcharacter: String::new(),
        };
        let mut in_line = false;
        let mut in_cell = false;
        let mut temp_format: Option<Format> = None;
        let mut end_cell = 0;

        let mut seen_linetypes = HashSet::new();

        for e in parser {
            match e {
                Ok(XmlEvent::StartElement {
                    name, attributes, ..
                }) => match name.local_name.as_str() {
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
                            padcharacter: String::new(),
                        };
                        for attr in attributes {
                            match attr.name.local_name.as_str() {
                                "linetype" => 
                                {
                                    if seen_linetypes.contains(&attr.value) {
                                        return Err(anyhow!("Duplicate linetype: {}", attr.value));
                                    }
                                    seen_linetypes.insert(attr.value.clone());
                                    temp_line.linetype = attr.value;
                                },
                                "occurs" => temp_line.occurs = attr.value, // TODO: not used by the parser yet.
                                "maxlength" => {
                                    temp_line.maxlength = attr.value.parse().unwrap_or(0)
                                }
                                "minlength" => {
                                    // TODO: not used by the parser yet.
                                    temp_line.minlength = attr.value.parse().unwrap_or(0)
                                }
                                "padcharacter" => {
                                    // TODO: not used by the parser yet.
                                    temp_line.padcharacter = attr.value
                                }
                                _ => (),
                            }
                        }
                    }
                    "cell" if in_line => {
                        in_cell = true;
                        let mut cell_name = String::new();
                        let mut cell_length = 0;
                        let mut cell_alignment = String::new();
                        let mut cell_padcharacter = String::new();

                        for attr in attributes {
                            match attr.name.local_name.as_str() {
                                "name" => cell_name = attr.value, // TODO: check unique name
                                "length" => cell_length = attr.value.parse().unwrap_or(0),
                                "alignment" => cell_alignment = attr.value, // TODO: not used by the parser yet.
                                "padcharacter" => cell_padcharacter = attr.value, // TODO: not used by the parser yet.
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
                let cells_with_condition: Vec<_> = line
                    .cell
                    .iter()
                    .filter(|cell| cell.linecondition_pattern.is_some())
                    .cloned()
                    .collect();
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
            .filter(|line| {
                line.cell
                    .iter()
                    .all(|cell| cell.linecondition_pattern.is_none())
            })
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
        let line = binding
            .lines
            .iter()
            .find(|line| line.linetype == linetype)
            .cloned();
        line
    }

    pub fn get_line_separator(&self) -> String {
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
}
