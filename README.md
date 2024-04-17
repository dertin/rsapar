## 👋 Overview <a name="overview"></a>

RSaPar is a Rust library for schema-based parsing and validation of structured data files, inspired by JSaPar for Java.

## 💻 Usage <a name="usage"></a>

To use `RSaPar`, you need to define a `ParserConfig` and then call the `parser()` method. 

Here's a step-by-step guide on how to get started:

1. **Add `RSaPar` to your project with:**
    ```bash
    cargo add rsapar
    ```
2. **Define the data schema:** Create a schema XML file (`schema.xml`) to describe the structure of your data. The schema format attempts to follow the same rules as JSaPar's schema format. Detailed documentation on the support and compatibility with the JSaPar schema format in RSaPar will be provided soon. An example schema can be found in the `example` folder.

3. **Configure the parser:** Set up the parser configuration with the path to your data file, the number of workers, and the path to your schema file.

    Example parser all lines:
    ```rust
    let config = rsapar::ParserConfig {
        file_path: "./example/data.txt".to_string(),
        file_schema: "./example/schema.xml".to_string(),
    };
    let n_workers = 4; // Number of workers to be used for parallel processing of all lines.
    
    let result: Result<(), Vec<rsapar::ValidationError>> = rsapar::parser_all(config, n_workers);

    match result {
        Ok(_) => println!("All lines are processed"),
        Err(errors) => {
            for error in errors {
                println!("Error at line {}: {}", error.line, error.message);
            }
        }
    }
    ```
    Example parser line by line:
    ```rust
    let config = rsapar::ParserConfig {
        file_path: "./example/data.txt".to_string(),
        file_schema: "./example/schema.xml".to_string(),
    };
    
    let lines = rsapar::parser(config);

    for line_result in lines {
        match line_result {
            Ok(processed_line) => println!("{:?}", processed_line),
            Err(e) => println!("Error processing line: {:?}", e),
        }
    }
    ```

This setup provides a brief overview of how to start using `RSaPar`. The schema structure is inspired by JSaPar, and more information on this alignment will be available in the future.

## 🚀 Roadmap <a name="roadmap"></a>

- [ ] Full support for validation of fixed-width files (v0.1.2)

For more details on upcoming features and releases, check out the [milestones](https://github.com/dertin/rsapar/milestones)


## 💫 Contributions <a name="contributions"></a>

Contributions make the open source community thrive. Your contributions to `RSaPar` are **greatly appreciated**!

To contribute, fork the repo, create your feature branch, and submit a pull request. For bugs or suggestions, please open an issue with the appropriate tag (`bug` for bugs, `enhancement` for improvements). Don’t forget to star the project!

Thank you for your support!

## 🧪 Testing <a name="testing"></a>

To run the tests and benchmarks, use the following commands:

```bash
cargo test
cargo bench
```

To use the latest development version of `RSaPar` in your project, add the following to your `Cargo.toml` file:

```toml
[dependencies]
rsapar = {git = "https://github.com/dertin/rsapar.git", branch = "main"}
```

Then, regularly run `cargo update -p rsapar` to fetch the latest `main` branch commit.

## 🪪 License <a name="license"></a>
Distributed under the MIT or Apache-2.0 License.

Please note that while `RSaPar` attempts to follow the same schema format rules as [JSaPar](https://github.com/org-tigris-jsapar/jsapar), it is a separate implementation and does not reuse the JSaPar codebase.
