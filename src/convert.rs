/// Conversion of parsed data to the desired output format based on a provided template.
/// Feature under development. This feature is experimental and may change in future versions.
/// [example template](example/convert_blocks.xml)

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
pub struct Convert {
    pub config: ConvertConfig,
    pub blocks: Vec<Block>,
}

#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct Block {
    id: usize,
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

#[derive(Debug)]
struct SumByCell {
    cell: String,
    value: f64,
}

#[derive(Debug)]
struct AvgByCell {
    cell: String,
    count: usize,
    total_sum: f64,
    avg: f64,
}
#[derive(Debug)]
struct CountByLinetype {
    linetype: String,
    count: usize,
}
#[derive(Debug, Default)]
struct ConfigFlagsSpecialPlaceholders {
    sum: Option<Vec<SumByCell>>,         // sum(cell)
    avg: Option<Vec<AvgByCell>>,         // avg(cell)
    count: Option<Vec<CountByLinetype>>, // count(linetype)
}

#[allow(dead_code)]
impl Convert {
    /// Creates a new instance of the `Convert` struct with the provided configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The `ConvertConfig` struct that contains the configuration settings.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a `Convert` instance if the creation is successful,
    /// otherwise returns an `Err` with the corresponding error message.
    ///
    pub fn new(config: ConvertConfig) -> Result<Self, Error> {
        let file = File::open(Path::new(&config.file_template_path))?;
        let xml_template = EventReader::new(BufReader::new(file));

        let mut blocks = Vec::new();
        let mut current_block = Block::default();
        let mut block_id = 0;

        // read XML template and store blocks
        for event in xml_template {
            match event {
                Ok(XmlEvent::StartElement { name, attributes, .. }) => {
                    if name.local_name == "block" {
                        current_block = Block::default();

                        block_id += 1;
                        current_block.id = block_id;
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
                            id: 0,
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

    /// Converts the parsed data into the desired output format based on the provided template.
    ///
    /// # Arguments
    ///
    /// * `parser` - A mutable reference to the `Parser` struct that contains the parsed data.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the conversion is successful, otherwise returns an `Err` with the corresponding error message.
    ///
    /// # Example
    ///
    /// ```no_run
    ///
    /// let tpl_config = ConvertConfig {
    ///     file_output_path: "./example/report_output.txt".to_string(),
    ///     file_template_path: "./example/convert_blocks.xml".to_string(),
    /// };
    ///
    /// let template = Convert::new(tpl_config).unwrap();
    ///
    /// let config = ParserConfig {
    ///     file_path: "./example/fixedwidth_data.txt".to_string(),
    ///     file_schema: "./example/fixedwidth_schema.xml".to_string(),
    /// };
    /// let mut parser = Parser::new(config).unwrap();
    ///
    /// template.convert(&mut parser).unwrap();
    /// ```
    ///
    pub fn convert(self, parser: &mut Parser) -> Result<(), Error> {
        let file_output_path = self.config.file_output_path.to_owned();

        let file_output = File::create(&file_output_path).unwrap();
        let mut file_output = BufWriter::new(file_output);

        let mut has_results = false;
        let mut is_block_for_line = false;

        let mut step_number = 0; // initial value for 'step' in 'condition' attribute of 'block' element in XML template
        let mut line_by_linetype = 0; // initial value for 'line' in 'condition' attribute of 'block' element in XML template
        let mut last_linetype = String::new(); // last linetype processed

        let mut config_special_placeholders = ConfigFlagsSpecialPlaceholders::default();

        // iterate over blocks to store special placeholders
        for block in self.blocks.iter() {
            let block_content = block.content.clone();
            let special_placeholders = Convert::get_regex_special_block_content(&block_content);

            if let Some(special_placeholders) = special_placeholders {
                for sp in special_placeholders.iter() {
                    let sp_name = sp.0.as_str();
                    let sp_arg = sp.1.to_owned();
                    match sp_name {
                        "sum" => {
                            // {{sum(cell)}}
                            config_special_placeholders.sum = Some(
                                sp_arg.iter().map(|cell| SumByCell { cell: cell.to_owned(), value: 0.0 }).collect(),
                            );
                        }
                        "avg" => {
                            // {{avg(cell)}}
                            config_special_placeholders.avg = Some(
                                sp_arg
                                    .iter()
                                    .map(|cell| AvgByCell { cell: cell.to_owned(), count: 0, total_sum: 0.0, avg: 0.0 })
                                    .collect(),
                            );
                        }
                        "count" => {
                            // {{count(linetype)}}
                            config_special_placeholders.count = Some(
                                sp_arg
                                    .iter()
                                    .map(|linetype| CountByLinetype { linetype: linetype.to_owned(), count: 0 })
                                    .collect(),
                            );
                        }
                        _ => {}
                    }
                }
            }
        }

        // iterate over processed lines for writing blocks to file
        parser.iter_mut().for_each(|result| match result {
            Ok(processed_line) => {
                has_results = true;

                // increment line iterator for 'step' in 'condition' attribute of 'block' element
                step_number += 1;

                // increment line number by linetype for 'line' in 'condition' attribute of 'block' element
                if last_linetype != processed_line.linetype {
                    line_by_linetype = 1;
                    last_linetype = processed_line.linetype.clone();
                } else {
                    line_by_linetype += 1;
                }

                // store count by linetype for special placeholders | {{count(linetype)}}
                if let Some(count_linetypes) = &mut config_special_placeholders.count {
                    for count_by_linetype in count_linetypes {
                        if count_by_linetype.linetype == processed_line.linetype {
                            count_by_linetype.count += 1;
                        }
                    }
                }

                // store sum by cell for special placeholders | {{sum(cell)}}
                if let Some(sum_cells) = &mut config_special_placeholders.sum {
                    for sum_by_cell in sum_cells {
                        if let Some(cell_value) = processed_line.cell_values.get(&sum_by_cell.cell) {
                            let cell_value_as_float = cell_value.replace(',', ".").parse::<f64>().unwrap_or(0.0);
                            sum_by_cell.value += cell_value_as_float;
                        }
                    }
                }

                // store avg by cell for special placeholders | {{avg(cell)}}
                if let Some(avg_cells) = &mut config_special_placeholders.avg {
                    for avg_by_cell in avg_cells {
                        if let Some(cell_value) = processed_line.cell_values.get(&avg_by_cell.cell) {
                            let cell_value_as_float = cell_value.replace(',', ".").parse::<f64>().unwrap_or(0.0);
                            avg_by_cell.count += 1;
                            avg_by_cell.total_sum += cell_value_as_float;
                            avg_by_cell.avg = avg_by_cell.total_sum / avg_by_cell.count as f64;
                        }
                    }
                }

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

        // calculate average per cell | {{avg(cell)}}
        if let Some(avg_cells) = &mut config_special_placeholders.avg {
            for avg_by_cell in avg_cells {
                if avg_by_cell.count > 0 {
                    avg_by_cell.avg = avg_by_cell.total_sum / avg_by_cell.count as f64;
                }
            }
        }

        // XXX: only write EOF block if there are lines in the result
        if has_results {
            // write EOF block
            for block in self.blocks.iter() {
                if block.condition == "EOF" {
                    file_output.write_all(block.content.as_bytes()).unwrap();
                }
            }

            file_output.flush().unwrap(); // TODO: handle error
            
            self.render_special_placeholders(&file_output_path, &config_special_placeholders).unwrap();
        }

        Ok(())
    }

    /// Parses the block content and extracts regex patterns for each placeholder.
    ///
    /// This function takes the block content as input and extracts regex patterns for each placeholder in the content.
    ///
    /// # Arguments
    ///
    /// * `block_content` - A string slice containing the block content to parse.
    ///
    /// # Returns
    ///
    /// An `Option<HashMap<String, Regex>>` where each key is a placeholder and the value is the corresponding regex pattern.
    ///
    fn get_regex_block_content(block_content: &str) -> Option<HashMap<String, Regex>> {
        let mut regex_map: HashMap<String, Regex> = HashMap::new();
        let placeholder_pattern = Regex::new(r"\{\{(\w+)\}\}").unwrap();

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

    fn get_regex_special_block_content(block_content: &str) -> Option<HashMap<String, Vec<String>>> {
        let re = Regex::new(r"\{\{(\s*\w+\s*)\((.*?)\)\}\}").unwrap();
        let mut map: HashMap<String, Vec<String>> = HashMap::new();

        for cap in re.captures_iter(block_content) {
            let function = cap[1].trim().to_string(); // sum, avg, count
            let argument = cap[2].trim().to_string();

            map.entry(function).or_default().push(argument);
        }

        if map.is_empty() {
            None
        } else {
            Some(map)
        }
    }

    /// Renders placeholders in the given `block` content using values from the `processed_line`.
    ///
    /// This function iterates over placeholders defined in the `block` regex map and replaces them with the corresponding values from `processed_line`.
    ///
    /// # Arguments
    ///
    /// * `processed_line` - A reference to the processed line containing values to replace placeholders.
    /// * `block` - A reference to the block containing content with placeholders to replace.
    ///
    /// # Returns
    ///
    /// A `String` with placeholders replaced by values from the processed line.
    ///
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
            .replace("\\n", "\n") // new line
            .replace("\\t", "\t") // tab
            .replace("\\r", "\r") // carriage return
            .replace("\\0", "\0") // null
            .replace("\\f", "\x0C") // page break
    }

    /// Replaces placeholders in each line of the input file based on a set of predefined patterns.
    ///
    /// Currently, the only supported placeholder is `len(<linetype>)`, which will be replaced by the count of lines
    /// of the specified `<linetype>`.
    ///
    /// It uses a temporary file to save the modified content and then replaces the original input file with it.
    ///
    /// # Arguments
    ///
    /// * `file_path` - The path to the input file.
    /// * `count_by_linetype` - A map of `<linetype>` to its corresponding count of lines of that type.
    ///
    /// # Returns
    ///
    /// A Result indicating whether the operation succeeded or failed.
    ///
    fn render_special_placeholders(
        &self, file_path: &str, config_special_placeholders: &ConfigFlagsSpecialPlaceholders,
    ) -> Result<(), Error> {
        let file_path = Path::new(&file_path);
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);

        let temp_file_path = file_path.with_extension("tmp");
        let mut temp_file = OpenOptions::new().write(true).create(true).truncate(true).open(&temp_file_path)?;

        // compiled regex patterns for each special placeholder
        let mut regex_map: HashMap<String, Regex> = HashMap::new();

        if let Some(sum_cells) = &config_special_placeholders.sum {
            for sum_by_cell in sum_cells {
                let re_pattern = format!(r"\{{\{{\s*sum\({}\)\s*\}}\}}", regex::escape(&sum_by_cell.cell));
                let re = Regex::new(&re_pattern).unwrap();
                let key = format!("sum_{}", sum_by_cell.cell);
                regex_map.insert(key, re);
            }
        }

        if let Some(avg_cells) = &config_special_placeholders.avg {
            for avg_by_cell in avg_cells {
                let re_pattern = format!(r"\{{\{{\s*avg\({}\)\s*\}}\}}", regex::escape(&avg_by_cell.cell));
                let re = Regex::new(&re_pattern).unwrap();
                let key = format!("avg_{}", avg_by_cell.cell);
                regex_map.insert(key, re);
            }
        }

        if let Some(count_linetypes) = &config_special_placeholders.count {
            for count_by_linetype in count_linetypes {
                let re_pattern = format!(r"\{{\{{\s*count\({}\)\s*\}}\}}", regex::escape(&count_by_linetype.linetype));
                let re = Regex::new(&re_pattern).unwrap();
                let key = format!("count_{}", count_by_linetype.linetype);
                regex_map.insert(key, re);
            }
        }

        // iterate over lines in the input file for replacing special placeholders
        for line in reader.lines() {
            let line = line?;
            let mut content = line.clone();

            // replace placeholders with computed values
            if let Some(sum_cells) = &config_special_placeholders.sum {
                for sum_by_cell in sum_cells {
                    let key = format!("sum_{}", sum_by_cell.cell);
                    if let Some(re) = regex_map.get(&key) {
                        let replacement = sum_by_cell.value.to_string();
                        content = re.replace_all(&content, replacement.as_str()).to_string();
                    }
                }
            }

            if let Some(avg_cells) = &config_special_placeholders.avg {
                for avg_by_cell in avg_cells {
                    let key = format!("avg_{}", avg_by_cell.cell);
                    if let Some(re) = regex_map.get(&key) {
                        let replacement = avg_by_cell.avg.to_string();
                        content = re.replace_all(&content, replacement.as_str()).to_string();
                    }
                }
            }

            if let Some(count_linetypes) = &config_special_placeholders.count {
                for count_by_linetype in count_linetypes {
                    let key = format!("count_{}", count_by_linetype.linetype);
                    if let Some(re) = regex_map.get(&key) {
                        let replacement = count_by_linetype.count.to_string();
                        content = re.replace_all(&content, replacement.as_str()).to_string();
                    }
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
