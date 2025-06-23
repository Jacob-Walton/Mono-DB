use crate::completer::NsqlCompleter;
use rustyline::history::{DefaultHistory, History, SearchDirection};
use rustyline::{Editor, Result};

pub struct HistoryManager {
    history_file: String,
}

impl HistoryManager {
    pub fn new(history_file: &str) -> Result<Self> {
        Ok(Self {
            history_file: history_file.to_string(),
        })
    }

    pub fn load(&self, editor: &mut Editor<NsqlCompleter, DefaultHistory>) -> Result<()> {
        let _ = editor.load_history(&self.history_file);
        Ok(())
    }

    pub fn save(&self, editor: &mut Editor<NsqlCompleter, DefaultHistory>) -> Result<()> {
        editor.save_history(&self.history_file)
    }

    pub fn display(&self, editor: &Editor<NsqlCompleter, DefaultHistory>) -> Result<()> {
        let history_len = editor.history().len();

        if history_len == 0 {
            println!("No history available");
            return Ok(());
        }

        println!("\nQuery History:");
        println!("{}", "─".repeat(60));

        // Show last 20 entries
        let start = if history_len > 20 {
            history_len - 20
        } else {
            0
        };

        for i in start..history_len {
            if let Ok(Some(search_result)) = editor.history().get(i, SearchDirection::Forward) {
                let entry_str = search_result.entry;
                let display_entry = if entry_str.len() > 60 {
                    format!("{}...", &entry_str[..57])
                } else {
                    entry_str.to_string()
                };

                println!("{:4} │ {}", i + 1, display_entry);
            }
        }

        println!("{}", "─".repeat(60));
        println!("Use .history <n> to execute entry n");

        Ok(())
    }

    pub fn get_entry(
        &self,
        editor: &Editor<NsqlCompleter, DefaultHistory>,
        n: usize,
    ) -> Option<String> {
        if n > 0 && n <= editor.history().len() {
            editor
                .history()
                .get(n - 1, SearchDirection::Forward)
                .ok()
                .flatten()
                .map(|search_result| search_result.entry.to_string())
        } else {
            None
        }
    }
}
