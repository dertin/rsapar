mod decimal_format;
mod parser;
mod schema;

pub use decimal_format::*;
pub use parser::*;
pub use schema::*;

pub fn parser_all(
    config: parser::ParserConfig,
    n_workers: usize,
) -> Result<(), Vec<ProcessedLine>> {
    parser::Parser::new(config).parser_all(n_workers)
}

pub fn parser(config: parser::ParserConfig) -> ProcessedLinesIterator {
    let parser = parser::Parser::new(config);
    parser.into_iter()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema() {
        let schema: schema::Schema =
            schema::Schema::load("./example/schema.xml").expect("Failed to load schema");
        
        assert!(schema.fixedwidthschema.is_some());
    }
    #[test]
    fn test_parser_all() {
        let config = crate::ParserConfig {
            file_path: "./example/data.txt".to_string(),
            file_schema: "./example/schema.xml".to_string(),
        };

        let result: Result<(), Vec<ProcessedLine>> = crate::parser_all(config, 4);

        match result {
            Ok(_) => println!("All lines are processed"),
            Err(errors) => {
                for error in errors {
                    println!("Error at line {}: {}", error.line, error.message);
                }
            }
        }
    }
    #[test]
    fn test_parser() {
        let config = crate::ParserConfig {
            file_path: "./example/data.txt".to_string(),
            file_schema: "./example/schema.xml".to_string(),
        };

        let lines = crate::parser(config);

        for line_result in lines {
            match line_result {
                Ok(processed_line) => println!("{:?}", processed_line),
                Err(e) => eprintln!("Error processing line: {:?}", e),
            }
        }
    }

    #[test]
    fn test_validate_number() {
        // # is optional digit, 0 is required digit
        let pattern = "0,##0.00;(#,##0.000)";
        let formatter = decimal_format::DecimalFormat::new(pattern).unwrap();
        assert!(formatter.validate_number("2,234.56").is_ok());
        assert!(formatter.validate_number("-1,234.560").is_ok());
        assert!(formatter.validate_number("1234.56").is_err());
        assert!(formatter.validate_number("1234").is_err());

        let pattern = "0.#0,##0";
        let formatter = decimal_format::DecimalFormat::new(pattern).unwrap();
        assert!(formatter.validate_number("2.20,125").is_ok());

        let pattern = "';#'##0";
        let formatter = decimal_format::DecimalFormat::new(pattern).unwrap();
        assert!(formatter.validate_number(";#123").is_ok());

        let pattern = "#######0.00";
        let formatter = decimal_format::DecimalFormat::new(pattern).unwrap();
        assert!(formatter.validate_number("00204000.00").is_ok());
    }
}
