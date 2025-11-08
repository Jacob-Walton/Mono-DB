use gpui::*;

use crate::theme;

/// Icon component for rendering Font Awesome icons
pub struct Icon {
    icon: char,
    size: Pixels,
    color: Hsla,
}

impl Icon {
    /// Create a new icon with default size and color
    pub fn new(icon: char) -> Self {
        Self {
            icon,
            size: px(16.0),
            color: rgb(theme::foreground()).into(),
        }
    }

    /// Set the icon size in pixels
    pub fn size(mut self, size: Pixels) -> Self {
        self.size = size;
        self
    }

    /// Set the icon color
    pub fn color(mut self, color: Hsla) -> Self {
        self.color = color;
        self
    }
}

impl IntoElement for Icon {
    type Element = Div;

    fn into_element(self) -> Self::Element {
        div()
            .text_color(self.color)
            .text_size(self.size)
            .font_family(theme::FONT_AWESOME_SOLID)
            .font_weight(FontWeight::BLACK) // Font Awesome 7 Solid uses weight 900
            .line_height(relative(1.0))
            .child(self.icon.to_string())
    }
}

// Font Awesome Solid icon constants (unicode values)
// See: https://fontawesome.com/icons for full list

/// Home icon (solid)
pub const ICON_HOME: char = '\u{f015}';

/// User icon (solid)
pub const ICON_USER: char = '\u{f007}';

/// Settings/Gear icon (solid)
pub const ICON_GEAR: char = '\u{f013}';

/// Search icon (solid)
pub const ICON_SEARCH: char = '\u{f002}';

/// Bars/Menu icon (solid)
pub const ICON_BARS: char = '\u{f0c9}';

/// X/Close icon (solid)
pub const ICON_XMARK: char = '\u{f00d}';

/// Plus icon (solid)
pub const ICON_PLUS: char = '\u{f067}';

/// Minus icon (solid)
pub const ICON_MINUS: char = '\u{f068}';

/// Check icon (solid)
pub const ICON_CHECK: char = '\u{f00c}';

/// Circle Check icon (solid)
pub const ICON_CIRCLE_CHECK: char = '\u{f058}';

/// Triangle Exclamation/Warning icon (solid)
pub const ICON_TRIANGLE_EXCLAMATION: char = '\u{f071}';

/// Circle Info icon (solid)
pub const ICON_CIRCLE_INFO: char = '\u{f05a}';

/// Trash icon (solid)
pub const ICON_TRASH: char = '\u{f1f8}';

/// Pen/Edit icon (solid)
pub const ICON_PEN: char = '\u{f304}';

/// Arrow Right icon (solid)
pub const ICON_ARROW_RIGHT: char = '\u{f061}';

/// Arrow Left icon (solid)
pub const ICON_ARROW_LEFT: char = '\u{f060}';

/// Chevron Right icon (solid)
pub const ICON_CHEVRON_RIGHT: char = '\u{f054}';

/// Chevron Left icon (solid)
pub const ICON_CHEVRON_LEFT: char = '\u{f053}';

/// Chevron Down icon (solid)
pub const ICON_CHEVRON_DOWN: char = '\u{f078}';

/// Chevron Up icon (solid)
pub const ICON_CHEVRON_UP: char = '\u{f077}';

/// Star icon (solid)
pub const ICON_STAR: char = '\u{f005}';

/// Heart icon (solid)
pub const ICON_HEART: char = '\u{f004}';

/// Bell icon (solid)
pub const ICON_BELL: char = '\u{f0f3}';

/// Envelope/Mail icon (solid)
pub const ICON_ENVELOPE: char = '\u{f0e0}';

/// File icon (solid)
pub const ICON_FILE: char = '\u{f15b}';

/// Folder icon (solid)
pub const ICON_FOLDER: char = '\u{f07b}';

/// Folder Open icon (solid)
pub const ICON_FOLDER_OPEN: char = '\u{f07c}';

/// Database icon (solid)
pub const ICON_DATABASE: char = '\u{f1c0}';

/// Table icon (solid)
pub const ICON_TABLE: char = '\u{f0ce}';

/// Download icon (solid)
pub const ICON_DOWNLOAD: char = '\u{f019}';

/// Upload icon (solid)
pub const ICON_UPLOAD: char = '\u{f093}';

/// Power Off icon (solid)
pub const ICON_POWER_OFF: char = '\u{f011}';

/// Sign Out icon (solid)
pub const ICON_RIGHT_FROM_BRACKET: char = '\u{f08b}';
