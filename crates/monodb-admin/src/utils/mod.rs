pub mod linux;
pub mod macos;
pub mod windows;

#[cfg(target_os = "windows")]
pub use windows::listen_for_theme_change;

#[cfg(target_os = "macos")]
pub use macos::listen_for_theme_change;

#[cfg(target_os = "linux")]
pub use linux::listen_for_theme_change;
