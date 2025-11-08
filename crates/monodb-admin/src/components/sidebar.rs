use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::StyledExt;

use crate::components::{
    App, ICON_CHEVRON_DOWN, ICON_CHEVRON_RIGHT, ICON_DATABASE, ICON_TABLE, Icon,
};
use crate::constants::{SIDEBAR_WIDTH, TITLEBAR_HEIGHT};
use crate::theme;

/// Application sidebar component
///
/// Database object explorer showing databases and collections in a tree structure.
/// Similar to pgAdmin's object browser.
pub struct SideBar {
    app: Entity<App>,
    expanded_databases: Vec<String>,
}

impl SideBar {
    pub fn new(app: Entity<App>) -> Self {
        Self {
            app,
            expanded_databases: vec!["sample_db".to_string()],
        }
    }

    /// Toggle a database's expanded state
    fn toggle_database(&mut self, db_name: &str, cx: &mut Context<Self>) {
        if let Some(pos) = self.expanded_databases.iter().position(|x| x == db_name) {
            self.expanded_databases.remove(pos);
        } else {
            self.expanded_databases.push(db_name.to_string());
        }
        cx.notify();
    }

    /// Check if a database is expanded
    fn is_expanded(&self, db_name: &str) -> bool {
        self.expanded_databases.contains(&db_name.to_string())
    }

    /// Renders a database node with expand/collapse chevron
    fn database_node(&mut self, db_name: &str, cx: &mut Context<Self>) -> impl IntoElement {
        let is_expanded = self.is_expanded(db_name);
        let db_name_owned = db_name.to_string();
        let db_name_display = db_name.to_string();
        let view = cx.entity();

        div()
            .flex()
            .flex_col()
            .w_full()
            .child(
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .gap_2()
                    .w_full()
                    .px_2()
                    .py_1p5()
                    .rounded(px(4.0))
                    .text_size(px(13.0))
                    .text_color(rgb(theme::foreground()))
                    .cursor_pointer()
                    .hover(|style| style.bg(rgb(theme::hover())))
                    .on_mouse_down(MouseButton::Left, move |_event, _window, cx| {
                        view.update(cx, |this, cx| {
                            this.toggle_database(&db_name_owned, cx);
                        });
                    })
                    .child(
                        Icon::new(if is_expanded {
                            ICON_CHEVRON_DOWN
                        } else {
                            ICON_CHEVRON_RIGHT
                        })
                        .size(px(12.0))
                        .color(rgb(theme::foreground_muted()).into()),
                    )
                    .child(
                        Icon::new(ICON_DATABASE)
                            .size(px(14.0))
                            .color(rgb(theme::foreground()).into()),
                    )
                    .child(db_name_display),
            )
            .when(is_expanded, |this| {
                this.child(
                    div()
                        .flex()
                        .flex_col()
                        .pl(px(24.0))
                        .gap_0p5()
                        .child(self.collection_node("users"))
                        .child(self.collection_node("products"))
                        .child(self.collection_node("orders")),
                )
            })
    }

    /// Renders a collection/table node
    fn collection_node(&self, collection_name: &str) -> impl IntoElement {
        let name = collection_name.to_string();

        div()
            .flex()
            .flex_row()
            .items_center()
            .gap_2()
            .w_full()
            .px_2()
            .py_1p5()
            .rounded(px(4.0))
            .text_size(px(13.0))
            .text_color(rgb(theme::foreground()))
            .cursor_pointer()
            .hover(|style| style.bg(rgb(theme::hover())))
            .child(
                Icon::new(ICON_TABLE)
                    .size(px(14.0))
                    .color(rgb(theme::foreground_muted()).into()),
            )
            .child(name)
    }
}

impl Render for SideBar {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .absolute()
            .top_0()
            .left_0()
            .bottom_0()
            .flex()
            .flex_col()
            .pt(px(TITLEBAR_HEIGHT))
            .w(px(SIDEBAR_WIDTH))
            .h_full()
            .bg(rgb(theme::surface()))
            .border_r_1()
            .border_color(rgb(theme::border()))
            .child(
                // Object Explorer
                div()
                    .flex()
                    .flex_col()
                    .flex_1()
                    .gap_1()
                    .p_3()
                    .scrollable(Axis::Vertical)
                    .child(
                        // Section label
                        div()
                            .text_size(px(11.0))
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(rgb(theme::foreground_muted()))
                            .px_2()
                            .py_2()
                            .child("DATABASES"),
                    )
                    .child(self.database_node("sample_db", cx))
                    .child(self.database_node("production_db", cx))
                    .child(self.database_node("test_db", cx)),
            )
    }
}
