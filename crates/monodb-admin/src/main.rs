//! MonoDB Admin - Desktop Interface

#![allow(unexpected_cfgs)]

use std::time::Duration;

use gpui::*;
use gpui_component::*;

mod actions;
mod components;
mod constants;
mod theme;
mod utils;

use actions::{Quit, quit};
use components::App;
use constants::{THEME_POLL_INTERVAL_MS, WINDOW_HEIGHT, WINDOW_WIDTH};
use theme::{init_fonts, init_theme};

/// Asset loader for the application.
///
/// Provides file system access for loading application assets such as fonts, icons, and images.
/// This implementation loads assets directly from the file system rather than embedding them.
struct Assets;

impl AssetSource for Assets {
    /// Loads an asset from the file system at the specified path.
    ///
    /// # Arguments
    ///
    /// * `path` - The file system path to the asset
    ///
    /// # Returns
    ///
    /// Returns the asset data as a byte array wrapped in `Option<Cow<'static, [u8]>>`,
    /// or an error if the file cannot be read.
    fn load(&self, path: &str) -> Result<Option<std::borrow::Cow<'static, [u8]>>> {
        std::fs::read(path)
            .map(Into::into)
            .map(Some)
            .map_err(Into::into)
    }

    /// Lists all files in a directory.
    ///
    /// # Arguments
    ///
    /// * `path` - The directory path to list
    ///
    /// # Returns
    ///
    /// Returns a vector of file names as `SharedString`, or an error if the directory
    /// cannot be read.
    fn list(&self, path: &str) -> Result<Vec<SharedString>> {
        Ok(std::fs::read_dir(path)?
            .filter_map(|entry| {
                entry.ok().and_then(|e| {
                    e.file_name()
                        .to_str()
                        .map(|s| SharedString::from(s.to_string()))
                })
            })
            .collect())
    }
}

/// Application entry point.
///
/// Initializes the GPUI application, sets up the theme system, loads fonts, and creates
/// the main application window. Also starts background tasks for theme monitoring.
fn main() {
    // Initialize GPUI application with custom asset loader
    let app = Application::new().with_assets(Assets);

    app.run(move |cx| {
        // Initialize theme system, detects OS theme (light/dark) and starts background listener
        init_theme();

        // Initialize the component framework
        gpui_component::init(cx);

        // Load Font Awesome fonts for icon rendering
        if let Err(e) = init_fonts(cx) {
            eprintln!("Failed to load fonts: {}", e);
        }

        // Register global actions and keyboard shortcuts
        cx.on_action(quit);
        cx.bind_keys([gpui::KeyBinding::new("escape", Quit, None)]);

        // Start background task to poll for theme changes and refresh UI when detected
        // This receives notifications from the OS-specific theme listener threads
        cx.spawn(async move |cx| {
            let receiver = theme::theme_receiver();

            loop {
                // Poll at configured interval
                cx.background_executor()
                    .timer(Duration::from_millis(THEME_POLL_INTERVAL_MS))
                    .await;

                // Check if a theme change event was received
                if receiver.try_recv().is_ok() {
                    // Refresh all windows to apply new theme
                    let _ = cx.update(|cx| {
                        cx.refresh_windows();
                    });
                }
            }
        })
        .detach();

        // Spawn task to create and display the main application window
        cx.spawn(async move |cx| {
            let window_size = size(px(WINDOW_WIDTH), px(WINDOW_HEIGHT));

            // Create window options with centered bounds
            let options = cx
                .update(|cx| {
                    let bounds = Bounds::centered(None, window_size, cx);
                    WindowOptions {
                        window_bounds: Some(WindowBounds::Windowed(bounds)),
                        focus: true,
                        show: true,
                        ..Default::default()
                    }
                })
                .ok();

            // Open the window with the App component as the root view
            cx.open_window(options.unwrap_or_default(), |window, cx| {
                // On macOS, explicitly focus the window to bring it to the foreground
                #[cfg(target_os = "macos")]
                utils::macos::focus_window(window);

                // Create the main App component and wrap it in a Root for rendering
                let view = cx.new(|_| App::default());
                cx.new(|cx| Root::new(view.into(), window, cx))
            })?;

            Ok::<_, anyhow::Error>(())
        })
        .detach();
    });
}
