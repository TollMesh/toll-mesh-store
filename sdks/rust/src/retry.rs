use crate::TollMeshError;
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;

pub struct RetryConfig {
    pub max_retries: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub jitter: bool,
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_millis(1000),
            max_delay: Duration::from_secs(60),
            jitter: true,
            backoff_multiplier: 2.0,
        }
    }
}

impl RetryConfig {
    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        let base = self.base_delay.as_millis() as f64;
        let mut delay = (base * self.backoff_multiplier.powi(attempt as i32))
            .min(self.max_delay.as_millis() as f64) as u64;

        if self.jitter {
            let jitter_amount = delay / 4;
            delay = (delay - jitter_amount + rand::random::<u64>() % (jitter_amount * 2))
                .max(0);
        }

        Duration::from_millis(delay)
    }
}

pub async fn with_retry<F, Fut, T>(
    mut f: F,
    config: &RetryConfig,
) -> Result<T, TollMeshError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, TollMeshError>>,
{
    let mut last_error = None;

    for attempt in 0..=config.max_retries {
        match f().await {
            Ok(result) => return Ok(result),
            Err(err) => {
                if !err.is_retryable() {
                    return Err(err);
                }

                last_error = Some(err);

                if attempt < config.max_retries {
                    let delay = config.calculate_delay(attempt);
                    sleep(delay).await;
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        TollMeshError::new(crate::ErrorCode::Internal, "Retry failed")
    }))
}
