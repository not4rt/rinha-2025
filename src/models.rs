use rust_decimal::Decimal;
use uuid::Uuid;

pub struct PaymentRequest {
    pub correlation_id: Uuid,
    pub amount: Decimal,
}

pub struct ProcessorPayment<'a> {
    pub correlation_id: Uuid,
    pub amount: Decimal,
    pub requested_at: &'a str,
}

pub struct PaymentSummary {
    pub default: ProcessorSummary,
    pub fallback: ProcessorSummary,
}

pub struct ProcessorSummary {
    pub total_amount: Decimal,
    pub total_requests: i64,
}

// pub struct ProcessorHealth {
//     pub failing: bool,
//     pub min_response_time: i32,
// }

#[derive(Clone, Copy, PartialEq)]
pub enum Processor {
    Default,
    Fallback,
}

// pub const PROCESSOR_DEFAULT: &str = "default";
// pub const PROCESSOR_FALLBACK: &str = "fallback";

impl Processor {
    // #[inline]
    // pub fn as_str(self) -> &'static str {
    //     match self {
    //         Processor::Default => PROCESSOR_DEFAULT,
    //         Processor::Fallback => PROCESSOR_FALLBACK,
    //     }
    // }
    #[inline]
    pub fn as_bool(self) -> bool {
        match self {
            Processor::Default => true,
            Processor::Fallback => false,
        }
    }
}
