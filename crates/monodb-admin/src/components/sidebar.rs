use gpui::*;
use gpui::prelude::FluentBuilder;

use crate::components::{
    App, Dialog, DialogContent, Icon,
    ICON_GEAR, ICON_HOME, ICON_RIGHT_FROM_BRACKET, ICON_FILE, ICON_SEARCH
};
use crate::constants::{TITLEBAR_HEIGHT, SIDEBAR_WIDTH};
use crate::theme;

/// Application sidebar component
///
/// Provides vertical navigation with main menu items and bottom action items.
/// Features hover states, proper spacing, and visual hierarchy.
pub struct SideBar {
    app: Entity<App>,
    active_item: Option<&'static str>,
}

impl SideBar {
    pub fn new(app: Entity<App>) -> Self {
        Self {
            app,
            active_item: Some("home"),
        }
    }

    /// Renders a navigation item with icon and label
    fn nav_item(
        &self,
        id: &'static str,
        icon: char,
        label: &'static str,
        is_active: bool,
    ) -> Div {
        let active_color = rgb(0x3B82F6);
        let active_bg = rgb(0x3B82F6).opacity(0.1);

        div()
            .id(id)
            .flex()
            .flex_row()
            .items_center()
            .gap_3()
            .w_full()
            .px_3()
            .py_2p5()
            .rounded(px(6.0))
            .text_size(px(13.0))
            .font_weight(FontWeight::MEDIUM)
            .cursor_pointer()
            .when(is_active, |style| {
                style
                    .bg(active_bg)
                    .text_color(active_color)
            })
            .when(!is_active, |style| {
                style
                    .text_color(rgb(theme::foreground()))
                    .hover(|s| s.bg(rgb(theme::hover())))
                    .active(|s| s.bg(rgb(theme::active())))
            })
            .child(
                Icon::new(icon)
                    .size(px(16.0))
                    .color(if is_active {
                        active_color.into()
                    } else {
                        rgb(theme::foreground()).into()
                    })
            )
            .child(label)
    }

    /// Renders a bottom action item (settings, logout)
    fn action_item(&self, icon: char, label: &'static str) -> Div {
        div()
            .flex()
            .flex_row()
            .items_center()
            .gap_3()
            .w_full()
            .px_3()
            .py_2p5()
            .rounded(px(6.0))
            .text_size(px(13.0))
            .font_weight(FontWeight::MEDIUM)
            .text_color(rgb(theme::foreground()))
            .cursor_pointer()
            .hover(|style| style.bg(rgb(theme::hover())))
            .active(|style| style.bg(rgb(theme::active())))
            .child(
                Icon::new(icon)
                    .size(px(16.0))
                    .color(rgb(theme::foreground()).into())
            )
            .child(label)
    }
}

impl Render for SideBar {
    fn render(&mut self, _: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        let app = self.app.clone();

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
                // Main navigation section
                div()
                    .flex()
                    .flex_col()
                    .flex_1()
                    .gap_1()
                    .p_3()
                    .child(
                        // Section label
                        div()
                            .text_size(px(11.0))
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(rgb(theme::foreground()).opacity(0.5))
                            .px_3()
                            .py_2()
                            .child("NAVIGATION")
                    )
                    .child(self.nav_item("home", ICON_HOME, "Home", self.active_item == Some("home")))
                    .child(self.nav_item("databases", ICON_FILE, "Databases", false))
                    .child(self.nav_item("query", ICON_SEARCH, "Query", false))
            )
            .child(
                // Bottom actions section
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .p_3()
                    .border_t_1()
                    .border_color(rgb(theme::border()))
                    .child({
                        let app = app.clone();
                        self.action_item(ICON_GEAR, "Settings")
                            .on_mouse_down(MouseButton::Left, move |_event, _window, cx| {
                                let dialog = cx.new(|_cx| Dialog::new(app.clone(), DialogContent::Settings));
                                app.update(cx, |this, cx| {
                                    this.push_dialog(dialog, cx);
                                });
                            })
                    })
                    .child(self.action_item(ICON_RIGHT_FROM_BRACKET, "Sign Out"))
            )
    }
}
