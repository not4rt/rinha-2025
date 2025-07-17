use ureq::Agent;

use crate::db_pool::HealthDbClient;
use crate::models::PROCESSOR_DEFAULT;
use crate::models::{Processor, Processor::Default, Processor::Fallback, ProcessorHealth};

pub struct HealthChecker {
    pub db: HealthDbClient,
    pub default_url: &'static str,
    pub fallback_url: &'static str,
    pub agent: Agent,
}

impl HealthChecker {
    pub fn new(db: HealthDbClient, default_url: &'static str, fallback_url: &'static str) -> Self {
        let agent = Agent::config_builder()
            .timeout_global(Some(std::time::Duration::from_secs(30)))
            .http_status_as_error(false)
            .build()
            .into();

        Self {
            db,
            default_url,
            fallback_url,
            agent,
        }
    }

    pub fn check_health(&self) {
        let res = self.db.with_transaction(|transaction| {
            let rows = transaction.query_raw(&self.db.statements.outdated_processors, &[])?;
            let all_rows = Vec::from_iter(rows.map(|r| r.unwrap()));
            if all_rows.is_empty() {
                return Ok(());
            }

            let mut processors = Vec::with_capacity(all_rows.len());
            processors.extend(all_rows.iter().map(|r| {
                if r.get::<usize, &str>(0) == PROCESSOR_DEFAULT {
                    Default
                } else {
                    Fallback
                }
            }));

            for processor in processors {
                let health_result = match self.get_health_check(processor) {
                    Ok(health) => health,
                    Err(e) => {
                        println!("get_health_check error: {e:?}");
                        continue;
                    }
                };

                transaction.query_raw(
                    &self.db.statements.update_health_check,
                    &[
                        &health_result.failing,
                        &health_result.min_response_time,
                        &processor.as_str(),
                    ],
                )?;
            }

            Ok(())
        });

        if let Err(e) = res {
            dbg!(e);
        }
    }

    fn get_health_check(&self, processor: Processor) -> Result<ProcessorHealth, ureq::Error> {
        let url = match processor {
            Default => self.default_url,
            Fallback => self.fallback_url,
        };
        let health_url = [url, "/payments/service-health"].concat();
        self.agent
            .get(&health_url)
            .call()?
            .body_mut()
            .read_json::<ProcessorHealth>()
    }
}
