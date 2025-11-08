#![allow(unused)]

use crossbeam_channel::{Receiver, Sender, unbounded};
use std::sync::{OnceLock, RwLock};

pub struct Theme {
    pub black: u32,
    pub white: u32,
    pub foreground: u32,
    pub foreground_muted: u32,
    pub background: u32,
    pub surface: u32,
    pub hover: u32,
    pub active: u32,
    pub border: u32,
    pub primary: u32,
    pub primary_bg: u32,
    pub primary_border: u32,
    pub danger: u32,
    pub danger_hover: u32,
    pub danger_active: u32,
    pub success: u32,
    pub success_hover: u32,
    pub success_active: u32,
    pub warning: u32,
    pub warning_hover: u32,
    pub warning_active: u32,
}

#[derive(Debug, Clone, Copy)]
pub enum ThemeEvent {
    Changed,
}

impl Theme {
    pub fn light() -> Self {
        Self {
            black: 0x09090B,
            white: 0xFAFAFA,
            foreground: 0x18181B,
            foreground_muted: 0x71717A,
            background: 0xFFFFFF,
            surface: 0xFAFAFA,
            hover: 0xF4F4F5,
            active: 0xE4E4E7,
            border: 0xE4E4E7,
            primary: 0x3B82F6,
            primary_bg: 0xEFF6FF,
            primary_border: 0xA1A1AA,
            danger: 0xDC2626,
            danger_hover: 0xB91C1C,
            danger_active: 0x991B1B,
            success: 0x059669,
            success_hover: 0x047857,
            success_active: 0x065F46,
            warning: 0xD97706,
            warning_hover: 0xB45309,
            warning_active: 0x92400E,
        }
    }

    pub fn dark() -> Self {
        Self {
            black: 0x09090B,
            white: 0xFAFAFA,
            foreground: 0xFAFAFA,
            foreground_muted: 0xA1A1AA,
            background: 0x09090B,
            surface: 0x18181B,
            hover: 0x27272A,
            active: 0x3F3F46,
            border: 0x27272A,
            primary: 0x3B82F6,
            primary_bg: 0x1E3A5F,
            primary_border: 0x52525B,
            danger: 0xFCA5A5,
            danger_hover: 0xF87171,
            danger_active: 0xEF4444,
            success: 0x6EE7B7,
            success_hover: 0x34D399,
            success_active: 0x10B981,
            warning: 0xFCD34D,
            warning_hover: 0xFBBF24,
            warning_active: 0xF59E0B,
        }
    }
}

static THEME: OnceLock<RwLock<Theme>> = OnceLock::new();
static THEME_CHANNEL: OnceLock<(Sender<ThemeEvent>, Receiver<ThemeEvent>)> = OnceLock::new();

fn get_theme_channel() -> &'static (Sender<ThemeEvent>, Receiver<ThemeEvent>) {
    THEME_CHANNEL.get_or_init(|| {
        let (tx, rx) = unbounded();
        (tx, rx)
    })
}

pub fn theme_receiver() -> &'static Receiver<ThemeEvent> {
    &get_theme_channel().1
}

pub fn set_theme_mode(is_dark: bool) {
    let sender = get_theme_channel().0.clone();
    if let Some(theme_lock) = THEME.get() {
        if let Ok(mut theme) = theme_lock.write() {
            *theme = if is_dark {
                Theme::dark()
            } else {
                Theme::light()
            };
            let _ = sender.send(ThemeEvent::Changed);
        }
    }
}

fn listen_for_theme_change() {
    let sender = get_theme_channel().0.clone();

    crate::utils::listen_for_theme_change(move |is_dark| {
        if let Some(theme_lock) = THEME.get() {
            if let Ok(mut theme) = theme_lock.write() {
                *theme = if is_dark {
                    Theme::dark()
                } else {
                    Theme::light()
                };

                let _ = sender.send(ThemeEvent::Changed);
            }
        }
    });
}

pub fn init_theme() {
    #[cfg(target_os = "windows")]
    {
        use crate::utils::windows::is_dark_mode;

        match is_dark_mode() {
            Ok(true) => {
                THEME.get_or_init(|| RwLock::new(Theme::dark()));
            }
            Ok(false) => {
                THEME.get_or_init(|| RwLock::new(Theme::light()));
            }
            Err(e) => {
                eprintln!("Error detecting theme: {e}");
                THEME.get_or_init(|| RwLock::new(Theme::light()));
            }
        }

        listen_for_theme_change();
    }

    #[cfg(target_os = "macos")]
    {
        use crate::utils::macos::is_dark_mode;

        match is_dark_mode() {
            Ok(true) => {
                THEME.get_or_init(|| RwLock::new(Theme::dark()));
            }
            Ok(false) => {
                THEME.get_or_init(|| RwLock::new(Theme::light()));
            }
            Err(e) => {
                eprintln!("Error detecting theme: {e}");
                THEME.get_or_init(|| RwLock::new(Theme::light()));
            }
        }

        listen_for_theme_change();
    }

    #[cfg(target_os = "linux")]
    {
        use crate::utils::linux::is_dark_mode;

        match is_dark_mode() {
            Ok(true) => {
                THEME.get_or_init(|| RwLock::new(Theme::dark()));
            }
            Ok(false) => {
                THEME.get_or_init(|| RwLock::new(Theme::light()));
            }
            Err(e) => {
                eprintln!("Error detecting theme: {e}");
                THEME.get_or_init(|| RwLock::new(Theme::light()));
            }
        }

        listen_for_theme_change();
    }

    #[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
    {
        // Fallback for other platforms (BSD, etc.)
        THEME.get_or_init(|| RwLock::new(Theme::light()));
    }
}

fn current_theme() -> std::sync::RwLockReadGuard<'static, Theme> {
    let lock = THEME.get_or_init(|| RwLock::new(Theme::light()));
    lock.read().unwrap()
}

pub fn black() -> u32 {
    current_theme().black
}
pub fn white() -> u32 {
    current_theme().white
}
pub fn foreground() -> u32 {
    current_theme().foreground
}
pub fn foreground_muted() -> u32 {
    current_theme().foreground_muted
}
pub fn background() -> u32 {
    current_theme().background
}
pub fn surface() -> u32 {
    current_theme().surface
}
pub fn hover() -> u32 {
    current_theme().hover
}
pub fn active() -> u32 {
    current_theme().active
}
pub fn border() -> u32 {
    current_theme().border
}
pub fn primary() -> u32 {
    current_theme().primary
}
pub fn primary_bg() -> u32 {
    current_theme().primary_bg
}
pub fn primary_border() -> u32 {
    current_theme().primary_border
}

pub fn danger() -> u32 {
    current_theme().danger
}
pub fn danger_hover() -> u32 {
    current_theme().danger_hover
}
pub fn danger_active() -> u32 {
    current_theme().danger_active
}

pub fn success() -> u32 {
    current_theme().success
}
pub fn success_hover() -> u32 {
    current_theme().success_hover
}
pub fn success_active() -> u32 {
    current_theme().success_active
}

pub fn warning() -> u32 {
    current_theme().warning
}
pub fn warning_hover() -> u32 {
    current_theme().warning_hover
}
pub fn warning_active() -> u32 {
    current_theme().warning_active
}
