use anyhow::Result;
use gpui::App;
use std::borrow::Cow;

/// Font Awesome Solid font family name
pub const FONT_AWESOME_SOLID: &str = "Font Awesome 7 Free";

/// Font Awesome Brands font family name
pub const FONT_AWESOME_BRANDS: &str = "Font Awesome 7 Brands";

/// Font Awesome Regular font family name
pub const FONT_AWESOME_REGULAR: &str = "Font Awesome 7 Free";

static FONT_AWESOME_SOLID_BYTES: &[u8] =
    include_bytes!("../assets/fonts/Font Awesome 7 Free-Solid-900.otf");

static FONT_AWESOME_BRANDS_BYTES: &[u8] =
    include_bytes!("../assets/fonts/Font Awesome 7 Brands-Regular-400.otf");
static FONT_AWESOME_REGULAR_BYTES: &[u8] =
    include_bytes!("../assets/fonts/Font Awesome 7 Free-Regular-400.otf");

/// Initialize custom fonts by loading them into the text system
///
/// This should be called once at application startup before opening any windows.
pub fn init_fonts(cx: &mut App) -> Result<()> {
    let text_system = cx.text_system().clone();

    text_system.add_fonts(vec![
        Cow::Borrowed(FONT_AWESOME_SOLID_BYTES),
        Cow::Borrowed(FONT_AWESOME_REGULAR_BYTES),
        Cow::Borrowed(FONT_AWESOME_BRANDS_BYTES),
    ])?;

    Ok(())
}
