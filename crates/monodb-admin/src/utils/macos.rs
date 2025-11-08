#![cfg(target_os = "macos")]

use cocoa::appkit::{NSApp, NSApplication};
use cocoa::base::{id, nil};
use cocoa::foundation::{NSAutoreleasePool, NSString};
use objc::declare::ClassDecl;
use objc::runtime::{Object, Sel};
use objc::{class, msg_send, sel, sel_impl};
use std::io;
use std::sync::{Arc, Mutex};

/// Checks if macOS is in dark mode
pub fn is_dark_mode() -> io::Result<bool> {
    // Use the `defaults` command to check the AppleInterfaceStyle setting
    // If it exists and equals "Dark", we're in dark mode
    let output = std::process::Command::new("defaults")
        .args(["read", "-g", "AppleInterfaceStyle"])
        .output();

    match output {
        Ok(output) => {
            let result = String::from_utf8_lossy(&output.stdout);
            Ok(result.trim() == "Dark")
        }
        Err(_) => {
            // If the command fails, the key doesn't exist, meaning light mode
            Ok(false)
        }
    }
}

/// Starts a background thread to listen for theme changes on macOS
///
/// This uses NSDistributedNotificationCenter to listen for
/// AppleInterfaceThemeChangedNotification, providing immediate notification
/// when the system theme changes (instead of polling).
pub fn listen_for_theme_change<F>(callback: F)
where
    F: FnMut(bool) + Send + 'static,
{
    std::thread::spawn(move || {
        unsafe {
            let _pool = NSAutoreleasePool::new(nil);

            // Share callback between Rust and Objective-C via Arc
            let callback = Arc::new(Mutex::new(callback));
            let callback_ptr = Arc::into_raw(callback) as *mut std::ffi::c_void;

            let superclass = class!(NSObject);
            let mut decl = ClassDecl::new("ThemeObserver", superclass).unwrap();
            extern "C" fn theme_changed(this: &Object, _cmd: Sel, _notification: id) {
                unsafe {
                    let callback_ptr = *this.get_ivar::<*mut std::ffi::c_void>("callback");
                    let callback =
                        Arc::from_raw(callback_ptr as *const Mutex<Box<dyn FnMut(bool) + Send>>);

                    if let Ok(is_dark) = is_dark_mode()
                        && let Ok(mut cb) = callback.lock()
                    {
                        cb(is_dark);
                    }

                    // Keep Arc alive for future notifications
                    let _ = Arc::into_raw(callback);
                }
            }

            decl.add_ivar::<*mut std::ffi::c_void>("callback");
            decl.add_method(
                sel!(themeChanged:),
                theme_changed as extern "C" fn(&Object, Sel, id),
            );

            let observer_class = decl.register();
            let observer: id = msg_send![observer_class, new];
            (*observer).set_ivar("callback", callback_ptr);

            let notification_center: id =
                msg_send![class!(NSDistributedNotificationCenter), defaultCenter];

            let notification_name =
                NSString::alloc(nil).init_str("AppleInterfaceThemeChangedNotification");

            let _: () = msg_send![
                notification_center,
                addObserver: observer
                selector: sel!(themeChanged:)
                name: notification_name
                object: nil
            ];

            // Run loop required for notification delivery
            let app = NSApplication::sharedApplication(nil);
            let _: () = msg_send![app, run];
        }
    });
}

/// Focuses the window on macOS
pub fn focus_window(window: &gpui::Window) {
    window.activate_window();

    unsafe {
        let app: cocoa::base::id = NSApp();
        let _: () = msg_send![app, activateIgnoringOtherApps: true];
    }
}
