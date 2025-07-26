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

#[inline(always)]
pub fn process_amount_to_cents(amount_str: &str) -> u64 {
    let parts: Vec<&str> = amount_str.split('.').collect();

    match parts.as_slice() {
        [whole] => whole.parse::<u64>().unwrap_or(0) * 100,
        [whole, decimal] => {
            let whole_cents = whole.parse::<u64>().unwrap_or(0) * 100;
            let decimal_cents = if decimal.len() >= 2 {
                decimal[..2].parse::<u64>().unwrap_or(0)
            } else if decimal.len() == 1 {
                decimal.parse::<u64>().unwrap_or(0) * 10
            } else {
                0
            };
            whole_cents + decimal_cents
        }
        _ => 0,
    }
}
