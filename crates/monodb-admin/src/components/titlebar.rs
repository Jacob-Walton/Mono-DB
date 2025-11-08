use gpui::*;
use indexmap::IndexMap;

use crate::constants::TITLEBAR_HEIGHT;
use crate::theme;

/// Application title bar component
///
/// Provides the main navigation menu and branding area at the top of the application window.
/// Features a logo/title section on the left and menu items for File, Edit, and View operations.
pub struct TitleBar {
    menu_items: IndexMap<&'static str, &'static str>,
    height: f32,
}

impl TitleBar {
    pub fn new() -> Self {
        let mut menu_items = IndexMap::new();
        menu_items.insert("file-menu", "File");
        menu_items.insert("edit-menu", "Edit");
        menu_items.insert("view-menu", "View");

        Self {
            menu_items,
            height: TITLEBAR_HEIGHT,
        }
    }

    /// Renders a menu button with hover state
    fn menu_button(&self, id: &'static str, label: &'static str) -> Div {
        div()
            .id(id)
            .flex()
            .items_center()
            .justify_center()
            .h_full()
            .px_3()
            .text_color(rgb(theme::foreground()))
            .text_size(px(13.0))
            .font_weight(FontWeight::MEDIUM)
            .cursor_pointer()
            .hover(|style| style.bg(rgb(theme::hover())))
            .active(|style| style.bg(rgb(theme::active())))
            .child(label)
    }
}

impl Render for TitleBar {
    fn render(&mut self, _: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .absolute()
            .top_0()
            .left_0()
            .right_0()
            .w_full()
            .flex()
            .flex_row()
            .items_center()
            .h(px(self.height))
            .bg(rgb(theme::surface()))
            .border_b_1()
            .border_color(rgb(theme::border()))
            .child(
                // Logo/Brand section
                div()
                    .flex()
                    .items_center()
                    .h_full()
                    .px_4()
                    .gap_2()
                    .border_r_1()
                    .border_color(rgb(theme::border()))
                    .child(
                        // Database icon (using a circle to represent DB)
                        div()
                            .flex()
                            .items_center()
                            .justify_center()
                            .w(px(20.0))
                            .h(px(20.0))
                            .rounded(px(4.0))
                            .bg(rgb(0x3B82F6))
                            .text_color(rgb(theme::white()))
                            .text_size(px(11.0))
                            .font_weight(FontWeight::BOLD)
                            .child("M")
                    )
                    .child(
                        div()
                            .text_color(rgb(theme::foreground()))
                            .text_size(px(13.0))
                            .font_weight(FontWeight::SEMIBOLD)
                            .child("MonoDB Admin")
                    )
            )
            .child(
                // Menu items
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .h_full()
                    .children(
                        self.menu_items
                            .iter()
                            .map(|(&id, &label)| self.menu_button(id, label))
                    )
            )
    }
}
