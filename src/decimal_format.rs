use regex::Regex;

pub struct DecimalFormat {
    positive_pattern: String,
    negative_pattern: String,
}
/// Convert DecimalFormat (Java) pattern to regex.
/// @see: [DecimalFormat](https://docs.oracle.com/javase/8/docs/api/java/text/DecimalFormat.html)
impl DecimalFormat {
    pub fn new(pattern: &str) -> Result<Self, String> {
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
        let negative_pattern = patterns
            .get(1)
            .map(|p| format!("-{}", p))
            .unwrap_or_else(|| format!("-{}", positive_pattern));

        Ok(DecimalFormat {
            positive_pattern,
            negative_pattern,
        })
    }

    pub fn validate_number(&self, input: &str) -> Result<(), &'static str> {
        let positive_regex = Self::pattern_to_regex(&self.positive_pattern)?;
        let negative_regex = Self::pattern_to_regex(&self.negative_pattern)?;

        let positive_re = Regex::new(&positive_regex).map_err(|_| "Invalid regex pattern")?;
        let negative_re = Regex::new(&negative_regex).map_err(|_| "Invalid regex pattern")?;

        if positive_re.is_match(input) || negative_re.is_match(input) {
            Ok(())
        } else {
            Err("Input does not match pattern")
        }
    }

    fn pattern_to_regex(pattern: &str) -> Result<String, &'static str> {
        let mut regex_pattern = String::new();
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
                    '0' => regex_pattern.push_str("\\d"), // Match a digit.
                    '#' => regex_pattern.push_str("\\d?"), // Match an optional digit.
                    ',' => regex_pattern.push_str("\\,?"),
                    '.' => regex_pattern.push_str("\\."),
                    ';' => regex_pattern.push_str("\\;"),
                    '¤' => regex_pattern.push_str("\\$"),
                    _ => regex_pattern.push(c),
                }
            }
        }

        regex_pattern.insert(0, '^');
        regex_pattern.push('$');
        Ok(regex_pattern)
    }
}
