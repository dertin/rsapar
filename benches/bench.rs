use rsapar::{DecimalFormat, Parser, ParserConfig, ProcessedLineError, Convert, ConvertConfig};
use rayon::iter::{ParallelBridge, ParallelIterator};

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_decimal_format_new(c: &mut Criterion) {
    c.bench_function("decimal_format_new", |b| {
        b.iter(|| {
            let pattern = "0,##0.00;(#,##0.000)";
            let formatter = DecimalFormat::new(pattern).unwrap();
            assert!(formatter.validate_number("2,234.56").is_ok());
            assert!(formatter.validate_number("-1,234.560").is_ok());
            assert!(formatter.validate_number("1234.56").is_err());
            assert!(formatter.validate_number("1234").is_err());
        })
    });
}

fn bench_parse(c: &mut Criterion) {
    c.bench_function("parse", |b| {
        b.iter(|| {
            let config = ParserConfig {
                file_path: "./example/fixedwidth_data.txt".to_string(),
                file_schema: "./example/fixedwidth_schema.xml".to_string(),
            };

            let mut parser = Parser::new(config).unwrap();

            for line_result in parser.iter_mut() {
                if line_result.is_err() {
                    panic!("Error processing line");
                }
            }
        })
    });
}

fn bench_parse_iter_par(c: &mut Criterion) {
    c.bench_function("parse_iter_par", |b| {
        b.iter(|| {
            let config = ParserConfig {
                file_path: "./example/fixedwidth_data.txt".to_string(),
                file_schema: "./example/fixedwidth_schema.xml".to_string(),
            };

            let mut parser = Parser::new(config).unwrap();
            let schema = parser.schema.clone();
            
            parser
                .lines()
                .par_bridge()
                .map(|read_line| {
                    match read_line {
                        Ok(read_line) => {
                            let line_number = read_line.line_number;
                            let line_content = read_line.line_content;

                            match schema.validate_line(line_number, line_content.to_owned()) {
                                Ok(processed_line) => Ok(processed_line),
                                Err(processed_line) => Err(processed_line),
                            }
                        }
                        Err(e) => Err(ProcessedLineError { line_number: 0, message: format!("{}", e) }),
                    }
                })
                .for_each(|result_processed_line| match result_processed_line {
                    Ok(_) => {}
                    Err(processed_line) => {
                        println!("{:?}", processed_line);
                    }
                });
        })
    });
}

fn bench_convert(c: &mut Criterion) {
    c.bench_function("convert", |b| {
        b.iter(|| {
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
            })
    });
}


criterion_group!(benches, bench_decimal_format_new, bench_parse, bench_parse_iter_par, bench_convert);
criterion_main!(benches);
