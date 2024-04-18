use {
    governor::{DefaultKeyedRateLimiter, Quota},
    std::{net::IpAddr, num::NonZeroU32},
};

pub struct ConnectionRateLimiter {
    limiter: DefaultKeyedRateLimiter<IpAddr>,
}

impl ConnectionRateLimiter {
    pub fn new(limit_per_minute: u32) -> Self {
        let quota = Quota::per_minute(NonZeroU32::new(limit_per_minute).unwrap()); // Adjust the rate limit as needed
        Self {
            limiter: DefaultKeyedRateLimiter::keyed(quota),
        }
    }

    pub fn check(&self, ip: &IpAddr) -> bool {
        // Acquire a permit from the rate limiter for the given IP address
        if self.limiter.check_key(ip).is_ok() {
            // Simulate making a request (e.g., establishing a TCP connection)
            debug!("Request from IP {:?} allowed", ip);
            true // Request allowed
        } else {
            debug!("Request from IP {:?} blocked", ip);
            false // Request blocked
        }
    }
}
