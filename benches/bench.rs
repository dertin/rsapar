use rsapar::DecimalFormat;

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
            let config = rsapar::ParserConfig {
                file_path: "./example/fixedwidth_data.txt".to_string(),
                file_schema: "./example/fixedwidth_schema.xml".to_string(),
            };

            let mut parser = rsapar::parser(config).unwrap();

            for line_result in parser.iter_mut() {
                if line_result.is_err() {
                    panic!("Error processing line");
                }
            }
        })
    });
}

criterion_group!(benches, bench_decimal_format_new, bench_parse);
criterion_main!(benches);
