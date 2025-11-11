use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::StyledExt;

use crate::components::{Dialog, SideBar, TitleBar};
use crate::theme;

pub struct App {
    dialog_stack: Vec<Entity<Dialog>>,
    sidebar: Option<Entity<SideBar>>,
    titlebar: Option<Entity<TitleBar>>,
}

impl Default for App {
    fn default() -> Self {
        Self {
            dialog_stack: Vec::new(),
            sidebar: None,
            titlebar: None,
        }
    }
}

impl App {
    pub fn push_dialog(&mut self, dialog: Entity<Dialog>, cx: &mut Context<Self>) {
        self.dialog_stack.push(dialog);
        cx.notify();
    }

    pub fn pop_dialog(&mut self, cx: &mut Context<Self>) {
        self.dialog_stack.pop();
        cx.notify();
    }
}

impl Render for App {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let app_view = cx.entity();
        let current_dialog = self.dialog_stack.last().cloned();

        // Initialize sidebar and titlebar once
        if self.sidebar.is_none() {
            self.sidebar = Some(cx.new(|_| SideBar::new(app_view.clone())));
        }
        if self.titlebar.is_none() {
            self.titlebar = Some(cx.new(|_| TitleBar::new(app_view.clone())));
        }

        div()
            .v_flex()
            .gap_2()
            .size_full()
            .items_center()
            .justify_center()
            .bg(rgb(theme::background()))
            .child(self.sidebar.clone().unwrap())
            .child(self.titlebar.clone().unwrap())
            .when_some(current_dialog, |this, dialog| this.child(dialog))
    }
}
