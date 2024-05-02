// Feature under development. This feature is experimental and may change in future versions.

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
};

use crate::{parser::Parser, ProcessedLineOk};
use anyhow::{anyhow, Error, Result};
use evalexpr::{ContextWithMutableVariables, HashMapContext};
use regex::Regex;
use xml::reader::{EventReader, XmlEvent};

#[derive(Debug)]
#[allow(dead_code)]
struct Convert {
    config: ConvertConfig,
    blocks: Vec<Block>,
}

#[derive(Debug)]
#[allow(dead_code)]
struct Block {
    condition: String,
    linetype: String,
    content: String,
    regex: Option<HashMap<String, Regex>>,
}

#[derive(Debug)]
pub struct ConvertConfig {
    pub file_output_path: String,
    pub file_template_path: String,
    // TODO: pub parser: Parser
}

#[allow(dead_code)]
impl Convert {
    pub fn new(config: ConvertConfig) -> Result<Self, Error> {
        let file = File::open(Path::new(&config.file_template_path))?;
        let xml_template = EventReader::new(BufReader::new(file));

        let mut blocks = Vec::new();
        let mut current_block = Block {
            condition: String::new(), // TODO: set default condition to None
            linetype: String::new(),  // TODO: set default linetype to None
            content: String::new(),
            regex: None,
        };

        // read XML template and store blocks
        for event in xml_template {
            match event {
                Ok(XmlEvent::StartElement { name, attributes, .. }) => {
                    if name.local_name == "block" {
                        for attr in attributes {
                            if attr.name.local_name == "condition" {
                                current_block.condition = attr.value;
                            } else if attr.name.local_name == "linetype" {
                                current_block.linetype = attr.value;
                            }
                        }
                    }
                }
                Ok(XmlEvent::Characters(content)) => {
                    let regex_blocks = Convert::get_regex_block_content(&content);
                    current_block.content = content;
                    current_block.regex = regex_blocks;
                }
                Ok(XmlEvent::EndElement { name }) => {
                    if name.local_name == "block" {
                        if current_block.content.is_empty() {
                            return Err(anyhow!("Block content is empty"));
                        }

                        blocks.push(current_block);
                        current_block = Block {
                            condition: String::new(),
                            linetype: String::new(),
                            content: String::new(),
                            regex: None,
                        };
                    }
                }
                Err(e) => {
                    return Err(anyhow!("Error parsing XML template: {}", e));
                }
                _ => {}
            }
        }

        Ok(Convert { config, blocks })
    }

    pub fn set_template(&mut self, _config: ConvertConfig) {
        todo!(); // TODO: move from constructor to this method
    }

    pub fn set_parser(&mut self, _parser: &mut Parser) {
        todo!(); // TODO: move from convert() to this method
    }

    pub fn convert(self, parser: &mut Parser) -> Result<(), Error> {
        let file_output_path = self.config.file_output_path.to_owned();

        let file_output = File::create(&file_output_path).unwrap();
        let mut file_output = BufWriter::new(file_output);

        let mut step_number = 0;
        let mut line_by_linetype = 0;
        let mut last_linetype = String::new();
        let mut count_by_linetype: HashMap<String, usize> = HashMap::new();
        let mut has_results = false;
        let mut is_block_for_line = false;

        // TODO: get list of special placeholders {{sum(cell)}}, {{avg(cell)}}, ... in block content

        // iterate over processed lines
        parser.iter_mut().for_each(|result| match result {
            Ok(processed_line) => {
                has_results = true;

                // increment step number
                step_number += 1;

                // increment line number by linetype
                if last_linetype != processed_line.linetype {
                    line_by_linetype = 1;
                    last_linetype = processed_line.linetype.clone();
                } else {
                    line_by_linetype += 1;
                }
                // store count by linetype
                count_by_linetype.insert(last_linetype.to_owned(), line_by_linetype);

                // iterate over blocks
                for block in self.blocks.iter() {
                    is_block_for_line = false;

                    // check if block condition matches line type
                    if block.linetype != processed_line.linetype && !block.linetype.is_empty() {
                        continue;
                    }

                    let mut context = HashMapContext::new();
                    // TODO: add only necessary variables to context depending on block content
                    context.set_value("step".into(), evalexpr::Value::Int(step_number as i64)).unwrap();
                    context.set_value("line".into(), evalexpr::Value::Int(line_by_linetype as i64)).unwrap();
                    context.set_value("EOF".into(), evalexpr::Value::Boolean(false)).unwrap();

                    // add more variables from processed_line
                    for (key, value) in processed_line.cell_values.iter() {
                        match context.set_value(key.to_owned(), evalexpr::Value::String(value.to_owned())) {
                            Ok(_) => {}
                            Err(e) => {
                                println!("Error setting value in context: {:?}", e);
                                // TODO: handle error
                                break;
                            }
                        }
                    }

                    // eval block condition with evalexpr
                    if !block.condition.is_empty() {
                        let condition = evalexpr::build_operator_tree(block.condition.as_str()).unwrap();

                        match condition.eval_with_context(&context) {
                            Ok(value) => {
                                if evalexpr::Value::Boolean(true) == value {
                                    is_block_for_line = true;
                                } else {
                                    continue; // this block is not for this line
                                }
                            }
                            Err(e) => {
                                println!("Error evaluating condition: {:?}", e);
                                // TODO: handle error
                                break;
                            }
                        }
                    } else {
                        is_block_for_line = true;
                    }

                    if is_block_for_line {
                        
                        let render_line = self.render_line_placeholders(&processed_line, block);

                        file_output.write_all(render_line.as_bytes()).unwrap();
                    }
                }
            }
            Err(processed_line) => {
                println!("Error processing line: {:?}", processed_line);
            }
        });

        // only write EOF block if there are lines in the result
        if has_results {
            // write EOF block
            for block in self.blocks.iter() {
                if block.condition == "EOF" {
                    file_output.write_all(block.content.as_bytes()).unwrap();
                }
            }

            file_output.flush().unwrap(); // TODO: handle error

            self.render_special_placeholders(&file_output_path, &count_by_linetype).unwrap();
        }

        Ok(())
    }

    fn get_regex_block_content(block_content: &str) -> Option<HashMap<String, Regex>> {
        let mut regex_map: HashMap<String, Regex> = HashMap::new();
        let placeholder_pattern = Regex::new(r"\{\{(\w+)\}\}").unwrap(); // Asume que los placeholders son palabras

        // compiled regex patterns for each placeholder
        for caps in placeholder_pattern.captures_iter(block_content) {
            let placeholder = caps.get(1).map_or("", |m| m.as_str());
            let format_placeholder = format!(r"\{{\{{{}\}}\}}", regex::escape(placeholder));
            let re = Regex::new(&format_placeholder).unwrap();
            regex_map.insert(placeholder.to_string(), re);
        }

        if regex_map.is_empty() {
            return None;
        }

        Some(regex_map)
    }

    fn render_line_placeholders(&self, processed_line: &ProcessedLineOk, block: &Block) -> String {
        let mut block_content = block.content.clone();

        for (key, value) in processed_line.cell_values.iter() {
            let block_regex = match block.regex {
                Some(ref regex_map) => regex_map,
                None => {
                    break;
                }
            };

            for (placeholder, re) in block_regex.iter() {
                if placeholder == key {
                    let replacement = value;
                    block_content = re.replace_all(&block_content, replacement).to_string();
                }
            }
        }

        block_content
            .replace("\\n", "\n")// new line
            .replace("\\t", "\t")// tab
            .replace("\\r", "\r") // carriage return
            .replace("\\0", "\0") // null
            .replace("\\f", "\x0C") // page break
    }

    /// Replaces placeholders like `{{len(_)}}` and other predefined patterns within each line of the input,
    /// with their corresponding computed values. This operation is performed after iterating over each line
    /// of the file, allowing for dynamic content modification based on the content of the entire file or external criteria.
    fn render_special_placeholders(&self, file_path: &str, count_by_linetype: &HashMap<String, usize>) -> Result<(), Error> {
        let file_path = Path::new(&file_path);
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);

        let temp_file_path = file_path.with_extension("tmp");
        let mut temp_file = OpenOptions::new().write(true).create(true).truncate(true).open(&temp_file_path)?;

        // compiled regex patterns for each special placeholder
        let mut regex_map: HashMap<String, Regex> = HashMap::new();
        for key in count_by_linetype.keys() {
            let re_pattern = format!(r"\{{\{{\s*len\({}\)\s*\}}\}}", regex::escape(key));
            let re = Regex::new(&re_pattern).unwrap();
            regex_map.insert(key.clone(), re);
        }

        for line in reader.lines() {
            let line = line?;
            let mut content = line.clone();

            // replace placeholders with computed values
            for (key, value) in count_by_linetype.iter() {
                if let Some(re) = regex_map.get(key) {
                    let replacement = value.to_string();
                    content = re.replace_all(&content, replacement.as_str()).to_string();
                }
            }

            writeln!(temp_file, "{}", content)?;
        }

        std::fs::rename(&temp_file_path, file_path)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{parser::Parser, ParserConfig};

    #[test]
    fn test_convert() {
        let file_output_path = "./example/report_output.txt";
        let file_template_path = "./example/convert_blocks.xml";
        let tpl_config = ConvertConfig {
            file_output_path: file_output_path.to_string(),
            file_template_path: file_template_path.to_string(),
        };

        let template = Convert::new(tpl_config).unwrap();
        assert!(!template.blocks.is_empty());

        let config = ParserConfig {
            file_path: "./example/fixedwidth_data.txt".to_string(),
            file_schema: "./example/fixedwidth_schema.xml".to_string(),
        };

        let mut parser = Parser::new(config).unwrap();

        template.convert(&mut parser).unwrap();
    }
}
