mod parser;
mod schema;

pub fn example() {
    let parser = parser::Parser::new(parser::ParserConfig {
        file_path: "./example/data.txt".to_string(),
        file_schema: "./example/schema.xml".to_string(),
        fn_worker: None,
        n_workers: 4,
    });
    parser.start().unwrap();
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
        assert!(result.is_ok(), "ERROR: {:?}", result.unwrap_err());
    }
}
