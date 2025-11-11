#![cfg(target_os = "linux")]

use std::io;

/// Checks if Linux desktop environment is in dark mode
///
/// This checks GNOME settings first (most common), then falls back to other methods.
/// Different desktop environments store theme preferences differently:
/// - GNOME: gsettings get org.gnome.desktop.interface color-scheme
/// - KDE: kreadconfig5 --group General --key ColorScheme
pub fn is_dark_mode() -> io::Result<bool> {
    // Try GNOME first (most common)
    if let Ok(is_dark) = check_gnome_dark_mode() {
        return Ok(is_dark);
    }

    // Try checking GTK theme name as fallback
    if let Ok(is_dark) = check_gtk_theme() {
        return Ok(is_dark);
    }

    // Default to light mode if we can't determine
    Ok(false)
}

/// Check GNOME dark mode setting via gsettings
fn check_gnome_dark_mode() -> io::Result<bool> {
    let output = std::process::Command::new("gsettings")
        .args(["get", "org.gnome.desktop.interface", "color-scheme"])
        .output()?;

    if output.status.success() {
        let result = String::from_utf8_lossy(&output.stdout);
        // GNOME returns 'prefer-dark' for dark mode, 'prefer-light' or 'default' for light
        return Ok(result.contains("dark"));
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "gsettings not available",
    ))
}

/// Check GTK theme name for dark mode indicators
fn check_gtk_theme() -> io::Result<bool> {
    let output = std::process::Command::new("gsettings")
        .args(["get", "org.gnome.desktop.interface", "gtk-theme"])
        .output()?;

    if output.status.success() {
        let result = String::from_utf8_lossy(&output.stdout).to_lowercase();
        // Common dark theme naming conventions
        return Ok(result.contains("dark") || result.contains("noir"));
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "gsettings not available",
    ))
}

/// Starts a background thread to listen for theme changes on Linux
///
/// This uses D-Bus to listen for property changes from the Freedesktop portal.
/// The portal provides a standardized way to detect theme changes across
/// different desktop environments (GNOME, KDE, etc.).
pub fn listen_for_theme_change<F>(callback: F)
where
    F: FnMut(bool) + Send + 'static,
{
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        rt.block_on(async {
            if let Err(e) = listen_dbus_async(callback).await {
                eprintln!("Failed to listen for theme changes on Linux: {}", e);
                listen_polling_fallback();
            }
        });
    });
}

/// Async function to listen for D-Bus signals
async fn listen_dbus_async<F>(mut callback: F) -> zbus::Result<()>
where
    F: FnMut(bool) + Send + 'static,
{
    use zbus::Connection;
    use zbus::zvariant::{OwnedValue, Value};

    // Connect to the session bus
    let connection = Connection::session().await?;

    // Subscribe to SettingChanged signals from the Freedesktop portal
    let mut stream = connection
        .receive_signal(
            Some("org.freedesktop.portal.Desktop"),
            Some("/org/freedesktop/portal/desktop"),
            Some("org.freedesktop.portal.Settings"),
            Some("SettingChanged"),
        )
        .await?;

    // Listen for signals
    while let Some(signal) = stream.next().await {
        let body = signal.body();

        if let Ok((namespace, key, value)) = body.deserialize::<(String, String, OwnedValue)>() {
            if namespace == "org.freedesktop.appearance" && key == "color-scheme" {
                // Value: 0=no pref, 1=dark, 2=light
                if let Some(Value::U32(scheme)) = value.downcast_ref::<Value>() {
                    callback(*scheme == 1);
                } else if let Ok(scheme) = value.downcast_ref::<u32>() {
                    callback(*scheme == 1);
                }
            }
        }
    }

    Ok(())
}

/// Fallback polling implementation if D-Bus monitoring fails
fn listen_polling_fallback() {
    let mut last_dark_mode = is_dark_mode().unwrap_or(false);

    loop {
        std::thread::sleep(std::time::Duration::from_millis(500));

        if let Ok(is_dark) = is_dark_mode() {
            if is_dark != last_dark_mode {
                last_dark_mode = is_dark;
                eprintln!(
                    "Theme changed to: {}",
                    if is_dark { "dark" } else { "light" }
                );
            }
        }
    }
}
