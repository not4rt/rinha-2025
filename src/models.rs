use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct PaymentRequest {
    #[serde(rename = "correlationId")]
    pub correlation_id: Uuid,
    pub amount: Decimal,
}

#[derive(Serialize)]
pub struct ProcessorPayment<'a> {
    #[serde(rename = "correlationId")]
    pub correlation_id: Uuid,
    pub amount: Decimal,
    #[serde(rename = "requestedAt")]
    pub requested_at: &'a str,
}

#[derive(Serialize)]
pub struct PaymentSummary {
    pub default: ProcessorSummary,
    pub fallback: ProcessorSummary,
}

#[derive(Serialize)]
pub struct ProcessorSummary {
    #[serde(rename = "totalAmount")]
    pub total_amount: Decimal,
    #[serde(rename = "totalRequests")]
    pub total_requests: i64,
}

#[derive(Deserialize)]
pub struct ProcessorHealth {
    pub failing: bool,
    #[serde(rename = "minResponseTime")]
    pub min_response_time: i32,
}

#[derive(Clone, Copy, PartialEq)]
pub enum Processor {
    Default,
    Fallback,
}

pub const PROCESSOR_DEFAULT: &str = "default";
pub const PROCESSOR_FALLBACK: &str = "fallback";

impl Processor {
    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Processor::Default => PROCESSOR_DEFAULT,
            Processor::Fallback => PROCESSOR_FALLBACK,
        }
    }
    #[inline]
    pub fn as_bool(self) -> bool {
        match self {
            Processor::Default => true,
            Processor::Fallback => false,
        }
    }
}
