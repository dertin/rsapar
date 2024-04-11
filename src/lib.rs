mod decimal_format;
mod parser;
mod schema;

pub use parser::*;
pub use schema::*;
pub use decimal_format::*;

pub fn parser(config: parser::ParserConfig) -> Result<(), Vec<ValidationError>> {
    parser::Parser::new(config).start()
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
    fn test_parser() {
        let parser = parser::Parser::new(parser::ParserConfig {
            file_path: "./example/data.txt".to_string(),
            fn_worker: None,
            n_workers: 4,
            file_schema: "./example/schema.xml".to_string(),
        });
        let result = parser.start();

        match result {
            Ok(_) => (),
            Err(errors) => {
                for v in errors {
                    println!("Line {}: {:?}", v.line, v.message);
                }
                //panic!("Failed to parse file");
            }
        }

        //assert!(result.is_ok(), "ERROR: {:?}", result.unwrap_err());
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