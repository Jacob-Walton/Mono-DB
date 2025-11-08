use gpui::*;

actions!(app, [Quit]);

/// Handler for the Quit action
pub fn quit(_: &Quit, cx: &mut App) {
    cx.quit();
}
