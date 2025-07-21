use rust_decimal::Decimal;
// use serde::{Deserialize, Serialize};
// use uuid::Uuid;

// pub struct PaymentRequest {
//     pub correlation_id: Uuid,
//     pub amount: Decimal,
// }

// #[derive(Serialize, Deserialize)]
// pub struct ProcessorPayment<'a> {
//     #[serde(rename = "correlationId")]
//     pub correlation_id: Uuid,
//     pub amount: Decimal,
//     #[serde(rename = "requestedAt")]
//     pub requested_at: &'a str,
// }

pub struct PaymentSummary {
    pub default: ProcessorSummary,
    pub fallback: ProcessorSummary,
}

pub struct ProcessorSummary {
    pub total_requests: i64,
    pub total_amount: Decimal,
}

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

// #[derive(Serialize, Deserialize)]
// pub struct QueuedPayment<'a> {
//     pub id: &'a str,
//     #[serde(rename = "correlationId")]
//     pub correlation_id: Uuid,
//     pub amount: Decimal,
//     #[serde(rename = "requestedAt")]
//     pub requested_at: i64, // milliseconds
// }
