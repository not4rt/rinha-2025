pub enum Processor {
    Default,
    Fallback,
}

impl Processor {
    #[inline]
    pub fn as_str(&self) -> &'static str {
        match self {
            Processor::Default => "default",
            Processor::Fallback => "fallback",
        }
    }
}
