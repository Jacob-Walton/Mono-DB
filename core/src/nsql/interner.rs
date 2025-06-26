use string_interner::{DefaultBackend, StringInterner as Interner};

pub type InternerId = string_interner::DefaultSymbol;
pub type StringInterner = Interner<DefaultBackend>;

// Helper trait
pub trait Intern {
	fn intern(&self, interner: &mut StringInterner) -> InternerId;
}

impl Intern for &str {
	fn intern(&self, interner: &mut StringInterner) -> InternerId {
		interner.get_or_intern(self)
	}
}

impl Intern for String {
	fn intern(&self, interner: &mut StringInterner) -> InternerId {
		interner.get_or_intern(self)
	}
}
