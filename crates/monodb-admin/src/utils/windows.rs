#![cfg(target_os = "windows")]

use winreg::{RegKey, enums::HKEY_CURRENT_USER};

pub fn is_dark_mode() -> std::io::Result<bool> {
    let hkcu = RegKey::predef(HKEY_CURRENT_USER);
    let personalize =
        hkcu.open_subkey("Software\\Microsoft\\Windows\\CurrentVersion\\Themes\\Personalize")?;
    let value: u32 = personalize.get_value("AppsUseLightTheme")?;

    Ok(value == 0)
}

pub fn listen_for_theme_change<F>(mut callback: F)
where
    F: FnMut(bool) + Send + 'static,
{
    use std::thread;
    use windows::Win32::Foundation::*;
    use windows::Win32::System::Registry::*;
    use windows::Win32::System::Threading::*;

    thread::spawn(move || {
        let winreg_hkcu = RegKey::predef(winreg::enums::HKEY_CURRENT_USER);

        match winreg_hkcu
            .open_subkey("Software\\Microsoft\\Windows\\CurrentVersion\\Themes\\Personalize")
        {
            Ok(key) => loop {
                match unsafe { CreateEventW(None, true, false, None) } {
                    Ok(event) => {
                        let raw_handle = key.raw_handle() as *mut std::ffi::c_void;
                        let result = unsafe {
                            RegNotifyChangeKeyValue(
                                HKEY(raw_handle),
                                false,
                                REG_NOTIFY_CHANGE_LAST_SET,
                                Some(event),
                                true,
                            )
                        };

                        if result.is_ok() {
                            unsafe {
                                WaitForSingleObject(event, INFINITE);
                            }

                            if let Ok(is_dark) = is_dark_mode() {
                                callback(is_dark);
                            }
                        }

                        unsafe {
                            let _ = CloseHandle(event);
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to create event: {:?}", e);
                        break;
                    }
                }

                thread::sleep(std::time::Duration::from_millis(100));
            },
            Err(e) => {
                eprintln!("Failed to open registry key: {}", e);
            }
        }
    });
}
