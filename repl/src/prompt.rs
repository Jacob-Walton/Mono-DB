use super::display::DisplayConfig;
use colored::Colorize;

pub fn generate_prompt(in_multiline: bool, query_count: usize, config: &DisplayConfig) -> String {
    if in_multiline {
        if config.use_colors {
            "    ... ".bright_black().to_string()
        } else {
            "    ... ".to_string()
        }
    } else {
        let base = "nsql";

        if config.use_colors {
            format!(
                "{}{}> ",
                base.bright_cyan().bold(),
                format!("[{}]", query_count).bright_black()
            )
        } else {
            format!("{}[{}]> ", base, query_count)
        }
    }
}
