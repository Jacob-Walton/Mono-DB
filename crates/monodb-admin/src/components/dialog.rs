use std::time::Duration;

use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::StyledExt;

use crate::components::{App, ICON_XMARK, Icon};
use crate::theme;

const CHEVRON_SVG: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/assets/chevron-down.svg");

#[derive(Clone)]
pub enum DialogContent {
    Settings,
    Confirmation { title: String, message: String },
}

pub struct Dialog {
    app: Entity<App>,
    content: DialogContent,
    dropdown_open: bool,
}

impl Dialog {
    pub fn new(app: Entity<App>, content: DialogContent) -> Self {
        Self {
            app,
            content,
            dropdown_open: false,
        }
    }

    fn get_dimensions(&self) -> (Pixels, Pixels) {
        match &self.content {
            DialogContent::Settings => (px(500.0), px(400.0)),
            DialogContent::Confirmation { .. } => (px(400.0), px(200.0)),
        }
    }

    fn render_content(&mut self, cx: &mut Context<Self>) -> AnyElement {
        match &self.content {
            DialogContent::Settings => self.render_settings(cx).into_any_element(),
            DialogContent::Confirmation { title, message } => self
                .render_confirmation(title.clone(), message.clone())
                .into_any_element(),
        }
    }

    fn render_settings(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        let view = cx.entity();
        let is_dropdown_open = self.dropdown_open;

        div()
            .flex()
            .flex_col()
            .gap_4()
            .p_4()
            .child(
                div()
                    .text_xl()
                    .font_semibold()
                    .text_color(rgb(theme::foreground()))
                    .child("Settings"),
            )
            .child(
                div().flex().flex_col().gap_3().child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_2()
                        .child(
                            div()
                                .text_sm()
                                .font_semibold()
                                .text_color(rgb(theme::foreground()))
                                .child("Theme"),
                        )
                        .child(
                            div()
                                .relative()
                                .child({
                                    let view = view.clone();
                                    div()
                                        .flex()
                                        .items_center()
                                        .justify_between()
                                        .px_4()
                                        .py_2p5()
                                        .border_1()
                                        .border_color(rgb(theme::border()))
                                        .rounded(px(6.0))
                                        .bg(rgb(theme::surface()))
                                        .hover(|style| {
                                            style
                                                .bg(rgb(theme::hover()))
                                                .border_color(rgb(theme::primary_border()))
                                        })
                                        .cursor_pointer()
                                        .on_mouse_down(
                                            MouseButton::Left,
                                            move |_event, _window, cx| {
                                                view.update(cx, |this, cx| {
                                                    this.dropdown_open = !this.dropdown_open;
                                                    cx.notify();
                                                });
                                            },
                                        )
                                        .child(
                                            div()
                                                .text_sm()
                                                .text_color(rgb(theme::foreground()))
                                                .child(format!(
                                                    "{}",
                                                    if theme::background() == 0x09090B {
                                                        "Dark"
                                                    } else {
                                                        "Light"
                                                    }
                                                )),
                                        )
                                        .child(
                                            svg()
                                                .size(px(10.0))
                                                .path(CHEVRON_SVG)
                                                .text_color(rgb(theme::foreground()))
                                                .with_animation(
                                                    if is_dropdown_open {
                                                        "chevron-open"
                                                    } else {
                                                        "chevron-close"
                                                    },
                                                    Animation::new(Duration::from_millis(200))
                                                        .with_easing(ease_in_out),
                                                    move |svg_el, delta| {
                                                        // Animate rotation based on state
                                                        // When opening: 0 -> 180 degrees (0.0 -> 0.5)
                                                        // When closing: 180 -> 0 degrees (0.5 -> 0.0)
                                                        let rotation = if is_dropdown_open {
                                                            delta * 0.5 // 0 to 0.5 (0 to 180 degrees)
                                                        } else {
                                                            (1.0 - delta) * 0.5 // 0.5 to 0 (180 to 0 degrees)
                                                        };
                                                        svg_el.with_transformation(
                                                            Transformation::rotate(percentage(
                                                                rotation,
                                                            )),
                                                        )
                                                    },
                                                ),
                                        )
                                })
                                .when(is_dropdown_open, |this| {
                                    this.child(
                                        div()
                                            .absolute()
                                            .top(px(48.0))
                                            .left(px(0.0))
                                            .right(px(0.0))
                                            .flex()
                                            .flex_col()
                                            .gap_1()
                                            .p_1()
                                            .border_1()
                                            .border_color(rgb(theme::border()))
                                            .rounded(px(6.0))
                                            .bg(rgb(theme::surface()))
                                            .shadow_lg()
                                            .overflow_hidden()
                                            .child({
                                                let view = view.clone();
                                                let is_light = theme::background() != 0x09090B;
                                                div()
                                                    .px_3()
                                                    .py_2()
                                                    .rounded(px(4.0))
                                                    .when(is_light, |style| {
                                                        style.bg(rgb(theme::active()))
                                                    })
                                                    .hover(|style| style.bg(rgb(theme::hover())))
                                                    .cursor_pointer()
                                                    .on_mouse_down(
                                                        MouseButton::Left,
                                                        move |_event, _window, cx| {
                                                            crate::theme::set_theme_mode(false);
                                                            view.update(cx, |this, cx| {
                                                                this.dropdown_open = false;
                                                                cx.notify();
                                                            });
                                                        },
                                                    )
                                                    .child(
                                                        div()
                                                            .text_sm()
                                                            .text_color(rgb(theme::foreground()))
                                                            .child("Light"),
                                                    )
                                            })
                                            .child({
                                                let view = view.clone();
                                                let is_dark = theme::background() == 0x09090B;
                                                div()
                                                    .px_3()
                                                    .py_2()
                                                    .rounded(px(4.0))
                                                    .when(is_dark, |style| {
                                                        style.bg(rgb(theme::active()))
                                                    })
                                                    .hover(|style| style.bg(rgb(theme::hover())))
                                                    .cursor_pointer()
                                                    .on_mouse_down(
                                                        MouseButton::Left,
                                                        move |_event, _window, cx| {
                                                            crate::theme::set_theme_mode(true);
                                                            view.update(cx, |this, cx| {
                                                                this.dropdown_open = false;
                                                                cx.notify();
                                                            });
                                                        },
                                                    )
                                                    .child(
                                                        div()
                                                            .text_sm()
                                                            .text_color(rgb(theme::foreground()))
                                                            .child("Dark"),
                                                    )
                                            }),
                                    )
                                }),
                        ),
                ),
            )
    }

    fn render_confirmation(&self, title: String, message: String) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .gap_4()
            .p_4()
            .child(
                div()
                    .text_lg()
                    .font_semibold()
                    .text_color(rgb(theme::foreground()))
                    .child(title),
            )
            .child(
                div()
                    .text_sm()
                    .text_color(rgb(theme::foreground()))
                    .child(message),
            )
    }
}

impl Render for Dialog {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let app = self.app.clone();
        let (dialog_width, dialog_height) = self.get_dimensions();

        div()
            .absolute()
            .top(px(0.0))
            .left(px(0.0))
            .right(px(0.0))
            .bottom(px(0.0))
            .flex()
            .items_center()
            .justify_center()
            .bg(rgba(0x00000080))
            .on_mouse_down(MouseButton::Left, {
                let app = app.clone();
                move |event, window, cx| {
                    let viewport = window.viewport_size();

                    // Calculate dialog bounds (centered)
                    let dialog_left = (viewport.width - dialog_width) / 2.0;
                    let dialog_right = dialog_left + dialog_width;
                    let dialog_top = (viewport.height - dialog_height) / 2.0;
                    let dialog_bottom = dialog_top + dialog_height;

                    // Only dismiss if clicking outside the dialog
                    if event.position.x < dialog_left
                        || event.position.x > dialog_right
                        || event.position.y < dialog_top
                        || event.position.y > dialog_bottom
                    {
                        app.update(cx, |this, cx| {
                            this.pop_dialog(cx);
                        });
                    }
                }
            })
            .child(
                div()
                    .flex()
                    .flex_col()
                    .w(dialog_width)
                    .h(dialog_height)
                    .bg(rgb(theme::background()))
                    .border_1()
                    .border_color(rgb(theme::border()))
                    .rounded(px(8.0))
                    .shadow_lg()
                    .child({
                        let app = app.clone();
                        div().flex().flex_row().justify_end().p_2().child(
                            div()
                                .flex()
                                .items_center()
                                .justify_center()
                                .w(px(24.0))
                                .h(px(24.0))
                                .rounded(px(4.0))
                                .hover(|style| style.bg(rgb(theme::hover())))
                                .cursor_pointer()
                                .on_mouse_down(MouseButton::Left, move |_event, _window, cx| {
                                    app.update(cx, |this, cx| {
                                        this.pop_dialog(cx);
                                    });
                                })
                                .child(
                                    Icon::new(ICON_XMARK)
                                        .size(px(12.0))
                                        .color(rgb(theme::foreground()).into()),
                                ),
                        )
                    })
                    .child(self.render_content(cx)),
            )
    }
}
