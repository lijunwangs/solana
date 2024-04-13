use {std::time::Duration, tokio::time::Instant};

#[derive(Debug)]
pub struct RateLimiter {
    /// count of requests in an interval
    pub(crate) count: u64,

    /// Throttle start time
    throttle_start_instant: Instant,
    interval: Duration,
    limit: u64,
}

/// A naive rate limiter, to be replaced by using governor
impl RateLimiter {
    pub fn new(limit: u64, interval: Duration) -> Self {
        Self {
            count: 0,
            throttle_start_instant: Instant::now(),
            interval,
            limit,
        }
    }

    /// Reset the counter and throttling start instant if needed.
    pub fn reset_throttling_params_if_needed(&mut self) {
        if Instant::now().duration_since(self.throttle_start_instant) > self.interval {
            self.throttle_start_instant = Instant::now();
            self.count = 0;
        }
    }
    pub fn is_allowed(&mut self) -> bool {
        self.reset_throttling_params_if_needed();
        if self.count >= self.limit {
            return false;
        }

        self.count = self.count.saturating_add(1);
        true
    }

    pub fn throttle_start_instant(&self) -> &Instant {
        &self.throttle_start_instant
    }
}

#[cfg(test)]
pub mod test {
    use {super::*, tokio::time::sleep};

    #[tokio::test]
    async fn test_rate_limiter() {
        let mut limiter = RateLimiter::new(2, Duration::from_millis(100));
        assert!(limiter.is_allowed());
        assert!(limiter.is_allowed());
        assert!(!limiter.is_allowed());
        let instant1 = *limiter.throttle_start_instant();

        // sleep 150 ms, the throttle parameters should have been reset.
        sleep(Duration::from_millis(150)).await;
        assert!(limiter.is_allowed());
        assert!(limiter.is_allowed());
        assert!(!limiter.is_allowed());

        let instant2 = *limiter.throttle_start_instant();
        assert!(instant2 > instant1);
    }
}
