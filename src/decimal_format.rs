use regex::Regex;
use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

/// Represents a decimal format pattern and provides methods for validating numbers against the pattern.
#[derive(Clone)]
pub struct DecimalFormat {
    positive_regex: Regex,
    negative_regex: Regex,
}

static DECIMAL_FORMAT_CACHE: OnceLock<Mutex<HashMap<String, DecimalFormat>>> = OnceLock::new();

/// Convert DecimalFormat (Java) pattern to regex.
/// @see: [DecimalFormat](https://docs.oracle.com/javase/8/docs/api/java/text/DecimalFormat.html)
impl DecimalFormat {
    /// Creates a new DecimalFormat instance with the specified pattern.
    ///
    /// # Arguments
    ///
    /// * `pattern` - The pattern string in the DecimalFormat (Java) format.
    ///
    /// # Returns
    ///
    /// A Result containing the DecimalFormat instance if the pattern is valid, or an error message if the pattern is invalid.
    pub fn new(pattern: &str) -> Result<Self, String> {
        let cache = DECIMAL_FORMAT_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
        let mut cache_guard = cache.lock().unwrap();

        if let Some(decimal_format) = cache_guard.get(pattern) {
            return Ok(decimal_format.to_owned());
        }

        let special_chars = ['\'', '(', ')', '0', '.', ',', '#', ';', '¤', '%'];
        let mut in_quotes = false;
        let mut patterns = vec![String::new()];
        for c in pattern.chars() {
            if !in_quotes && c == '\'' {
                in_quotes = true;
                patterns.last_mut().unwrap().push(c);
                continue;
            }
            if in_quotes && c == '\'' {
                in_quotes = false;
                patterns.last_mut().unwrap().push(c);
                continue;
            }
            if !in_quotes && !special_chars.contains(&c) {
                return Err(format!("Invalid character: {}", c));
            }

            if c == ';' && !in_quotes {
                patterns.push(String::new()); // Start a new pattern.
            } else {
                patterns.last_mut().unwrap().push(c); // Append to the current pattern.
            }
        }

        if patterns.len() > 2 {
            return Err("Invalid pattern".to_string());
        }

        let positive_pattern = patterns.first().ok_or("Missing positive pattern")?.clone();
        let negative_pattern =
            patterns.get(1).map(|p| format!("-{}", p)).unwrap_or_else(|| format!("-{}", positive_pattern));

        let positive_pattern = Self::pattern_to_regex(&positive_pattern);
        let negative_pattern = Self::pattern_to_regex(&negative_pattern);

        let positive_regex = Regex::new(&positive_pattern).map_err(|_| "Invalid regex pattern")?;
        let negative_regex = Regex::new(&negative_pattern).map_err(|_| "Invalid regex pattern")?;

        let decimal_format = DecimalFormat { positive_regex, negative_regex };

        cache_guard.insert(pattern.to_string(), decimal_format.clone());

        Ok(decimal_format)
    }

    pub fn validate_number(&self, input: &str) -> Result<(), &'static str> {
        if self.positive_regex.is_match(input) || self.negative_regex.is_match(input) {
            Ok(())
        } else {
            Err("Input does not match pattern")
        }
    }
    /// Converts a DecimalFormat pattern to a regex pattern.
    fn pattern_to_regex(pattern: &str) -> String {
        let mut regex_pattern = "^".to_string();
        let mut in_quotes = false;

        for c in pattern.chars() {
            if !in_quotes && c == '\'' {
                in_quotes = true;
                continue;
            }
            if in_quotes && c == '\'' {
                in_quotes = false;
                continue;
            }

            if in_quotes {
                regex_pattern.push(c);
            } else {
                match c {
                    '0' => regex_pattern.push_str("\\d"),  // Match a digit.
                    '#' => regex_pattern.push_str("\\d?"), // Match an optional digit.
                    ',' => regex_pattern.push_str("\\,"),  
                    '.' => regex_pattern.push_str("\\."),  
                    ';' => regex_pattern.push_str("\\;"),
                    '¤' => regex_pattern.push_str("\\$"), /* TODO: Add the international */
                    // currency symbol.
                    _ => regex_pattern.push(c),
                }
            }
        }
        regex_pattern.push('$');
        regex_pattern
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_number() {
        // # is optional digit, 0 is required digit
        let pattern = "0,##0.00;(#,##0.000)";
        let formatter = DecimalFormat::new(pattern).unwrap();
        assert!(formatter.validate_number("2,234.56").is_ok());
        assert!(formatter.validate_number("-1,234.560").is_ok());
        assert!(formatter.validate_number("1234.56").is_err());
        assert!(formatter.validate_number("1234").is_err());

        let pattern = "0.#0,##0";
        let formatter = DecimalFormat::new(pattern).unwrap();
        assert!(formatter.validate_number("2.20,125").is_ok());

        let pattern = "';#'##0";
        let formatter = DecimalFormat::new(pattern).unwrap();
        assert!(formatter.validate_number(";#123").is_ok());

        let pattern = "#######0.00";
        let formatter = DecimalFormat::new(pattern).unwrap();
        assert!(formatter.validate_number("00204000.00").is_ok());
    }
}
