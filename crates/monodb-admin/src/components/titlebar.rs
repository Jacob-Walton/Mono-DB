use gpui::prelude::FluentBuilder;
use gpui::*;
use indexmap::IndexMap;

use crate::components::{App, Dialog, DialogContent};
use crate::constants::TITLEBAR_HEIGHT;
use crate::theme;

/// Application title bar component
///
/// Provides the main navigation menu and branding area at the top of the application window.
/// Features a logo/title section on the left and menu items for File, Edit, and View operations.
pub struct TitleBar {
    app: Entity<App>,
    menu_items: IndexMap<&'static str, &'static str>,
    height: f32,
    file_menu_open: bool,
}

impl TitleBar {
    pub fn new(app: Entity<App>) -> Self {
        let mut menu_items = IndexMap::new();
        menu_items.insert("edit-menu", "Edit");
        menu_items.insert("view-menu", "View");

        Self {
            app,
            menu_items,
            height: TITLEBAR_HEIGHT,
            file_menu_open: false,
        }
    }

    /// Renders a menu button with hover state
    fn menu_button(&self, id: &'static str, label: &'static str) -> impl IntoElement {
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
            .child(label)
    }

    /// Renders the File menu with dropdown
    fn file_menu(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        let view = cx.entity();
        let is_open = self.file_menu_open;
        let app = self.app.clone();

        div()
            .relative()
            .child({
                let view_clone = view.clone();
                div()
                    .flex()
                    .items_center()
                    .justify_center()
                    .h(px(self.height))
                    .px_3()
                    .text_color(rgb(theme::foreground()))
                    .text_size(px(13.0))
                    .font_weight(FontWeight::MEDIUM)
                    .cursor_pointer()
                    .when(is_open, |style| style.bg(rgb(theme::hover())))
                    .hover(|style| style.bg(rgb(theme::hover())))
                    .on_mouse_down(MouseButton::Left, move |_event, _window, cx| {
                        view_clone.update(cx, |this, cx| {
                            this.file_menu_open = !this.file_menu_open;
                            cx.notify();
                        });
                    })
                    .child("File")
            })
            .when(is_open, |this| {
                this.child(
                    div()
                        .absolute()
                        .top(px(self.height))
                        .left(px(0.0))
                        .flex()
                        .flex_col()
                        .min_w(px(180.0))
                        .py_1()
                        .bg(rgb(theme::surface()))
                        .border_1()
                        .border_color(rgb(theme::border()))
                        .rounded(px(4.0))
                        .shadow_lg()
                        .child({
                            let app = app.clone();
                            let view = view.clone();
                            div()
                                .px_3()
                                .py_2()
                                .text_size(px(13.0))
                                .text_color(rgb(theme::foreground()))
                                .cursor_pointer()
                                .hover(|style| style.bg(rgb(theme::hover())))
                                .on_mouse_down(MouseButton::Left, move |_event, _window, cx| {
                                    let dialog = cx.new(|_cx| {
                                        Dialog::new(app.clone(), DialogContent::Settings)
                                    });
                                    app.update(cx, |this, cx| {
                                        this.push_dialog(dialog, cx);
                                    });
                                    view.update(cx, |this, cx| {
                                        this.file_menu_open = false;
                                        cx.notify();
                                    });
                                })
                                .child("Settings")
                        }),
                )
            })
    }
}

impl Render for TitleBar {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
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
                            .bg(rgb(theme::primary()))
                            .text_color(rgb(theme::white()))
                            .text_size(px(11.0))
                            .font_weight(FontWeight::BOLD)
                            .child("M"),
                    )
                    .child(
                        div()
                            .text_color(rgb(theme::foreground()))
                            .text_size(px(13.0))
                            .font_weight(FontWeight::SEMIBOLD)
                            .child("MonoDB Admin"),
                    ),
            )
            .child(
                // Menu items
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .h_full()
                    .child(self.file_menu(cx))
                    .children(
                        self.menu_items
                            .iter()
                            .map(|(&id, &label)| self.menu_button(id, label)),
                    ),
            )
    }
}
