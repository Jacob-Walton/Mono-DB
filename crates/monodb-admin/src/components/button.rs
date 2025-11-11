use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::button::*;

use crate::theme;

pub struct AppButton;

impl AppButton {
    pub fn primary(id: &'static str, label: &'static str, cx: &App) -> Button {
        let custom = ButtonCustomVariant::new(cx)
            .color(rgb(theme::black()).into())
            .foreground(rgb(theme::white()).into())
            .border(rgb(theme::primary_border()).into())
            .hover(rgb(theme::hover()).into())
            .active(rgb(theme::active()).into())
            .shadow(true);

        Button::new(id)
            .label(label)
            .custom(custom)
            .rounded(ButtonRounded::Medium)
            .px_4()
            .py_2()
    }

    pub fn secondary(id: &'static str, label: &'static str, cx: &App) -> Button {
        let custom = ButtonCustomVariant::new(cx)
            .color(rgb(theme::surface()).into())
            .foreground(rgb(theme::foreground()).into())
            .border(rgb(theme::border()).into())
            .hover(rgb(theme::hover()).into())
            .active(rgb(theme::active()).into())
            .shadow(true);

        Button::new(id)
            .label(label)
            .custom(custom)
            .rounded(ButtonRounded::Medium)
            .px_4()
            .py_2()
    }

    pub fn danger(id: &'static str, label: &'static str, cx: &App) -> Button {
        let custom = ButtonCustomVariant::new(cx)
            .color(rgb(theme::danger()).into())
            .foreground(rgb(theme::white()).into())
            .border(rgb(theme::danger()).into())
            .hover(rgb(theme::danger_hover()).into())
            .active(rgb(theme::danger_active()).into())
            .shadow(true);

        Button::new(id)
            .label(label)
            .custom(custom)
            .rounded(ButtonRounded::Medium)
            .px_4()
            .py_2()
    }

    pub fn success(id: &'static str, label: &'static str, cx: &App) -> Button {
        let custom = ButtonCustomVariant::new(cx)
            .color(rgb(theme::success()).into())
            .foreground(rgb(theme::white()).into())
            .border(rgb(theme::success()).into())
            .hover(rgb(theme::success_hover()).into())
            .active(rgb(theme::success_active()).into())
            .shadow(true);

        Button::new(id)
            .label(label)
            .custom(custom)
            .rounded(ButtonRounded::Medium)
            .px_4()
            .py_2()
    }

    pub fn warning(id: &'static str, label: &'static str, cx: &App) -> Button {
        let custom = ButtonCustomVariant::new(cx)
            .color(rgb(theme::warning()).into())
            .foreground(rgb(theme::white()).into())
            .border(rgb(theme::warning()).into())
            .hover(rgb(theme::warning_hover()).into())
            .active(rgb(theme::warning_active()).into())
            .shadow(true);

        Button::new(id)
            .label(label)
            .custom(custom)
            .rounded(ButtonRounded::Medium)
            .px_4()
            .py_2()
    }

    pub fn ghost(id: &'static str, label: &'static str, cx: &App) -> Button {
        let custom = ButtonCustomVariant::new(cx)
            .color(cx.theme().transparent)
            .foreground(rgb(theme::foreground()).into())
            .border(cx.theme().transparent)
            .hover(rgb(theme::hover()).into())
            .active(rgb(theme::active()).into())
            .shadow(false);

        Button::new(id)
            .label(label)
            .custom(custom)
            .rounded(ButtonRounded::Small)
            .px_3()
            .py_1p5()
    }

    pub fn outline(id: &'static str, label: &'static str, cx: &App) -> Button {
        let custom = ButtonCustomVariant::new(cx)
            .color(cx.theme().transparent)
            .foreground(rgb(theme::foreground()).into())
            .border(rgb(theme::border()).into())
            .hover(rgb(theme::surface()).into())
            .active(rgb(theme::hover()).into())
            .shadow(false);

        Button::new(id)
            .label(label)
            .custom(custom)
            .rounded(ButtonRounded::Medium)
            .border_1()
            .px_4()
            .py_2()
    }
}
