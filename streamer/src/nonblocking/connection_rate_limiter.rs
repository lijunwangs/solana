use {
    governor::{DefaultDirectRateLimiter, DefaultKeyedRateLimiter, Quota, RateLimiter},
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
            debug!("Request from IP {:?} allowed", ip);
            true // Request allowed
        } else {
            debug!("Request from IP {:?} blocked", ip);
            false // Request blocked
        }
    }
}

/// Connection rate limiter for enforcing connection rates from
/// all clients.
pub struct TotalConnectionRateLimiter {
    limiter: DefaultDirectRateLimiter,
}

impl TotalConnectionRateLimiter {
    pub fn new(limit_per_second: u32) -> Self {
        let quota = Quota::per_second(NonZeroU32::new(limit_per_second).unwrap()); // Adjust the rate limit as needed
        Self {
            limiter: RateLimiter::direct(quota),
        }
    }

    pub fn check(&self, ip: &IpAddr) -> bool {
        if self.limiter.check().is_ok() {
            debug!("Request from IP {:?} allowed", ip);
            true // Request allowed
        } else {
            debug!("Request from IP {:?} blocked", ip);
            false // Request blocked
        }
    }
}
