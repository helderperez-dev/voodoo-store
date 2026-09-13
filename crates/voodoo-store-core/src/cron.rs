//! Minimal deterministic cron expression support for the Store scheduler.
//!
//! Expressions use the common five-field form in UTC:
//! `minute hour day-of-month month day-of-week`.
//! Supported syntax: `*`, single values, comma-separated lists, ranges, and
//! step values such as `*/5` or `10-30/5`.

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronExpression {
    minute: Field,
    hour: Field,
    day_of_month: Field,
    month: Field,
    day_of_week: Field,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Field {
    min: u8,
    max: u8,
    allowed: Vec<bool>,
    wildcard: bool,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CronError {
    #[error("cron expression must contain exactly five fields")]
    InvalidFieldCount,
    #[error("invalid cron field: {0}")]
    InvalidField(String),
    #[error("cron timestamp overflow")]
    TimeOverflow,
}

impl CronExpression {
    pub fn parse(expression: &str) -> Result<Self, CronError> {
        let parts = expression.split_whitespace().collect::<Vec<_>>();
        if parts.len() != 5 {
            return Err(CronError::InvalidFieldCount);
        }
        Ok(Self {
            minute: Field::parse(parts[0], 0, 59)?,
            hour: Field::parse(parts[1], 0, 23)?,
            day_of_month: Field::parse(parts[2], 1, 31)?,
            month: Field::parse(parts[3], 1, 12)?,
            day_of_week: Field::parse(parts[4], 0, 6)?,
        })
    }

    /// Returns whether a UTC unix timestamp in milliseconds matches this cron.
    pub fn matches_utc_ms(&self, timestamp_ms: i64) -> bool {
        let seconds = timestamp_ms.div_euclid(1_000);
        let minutes = seconds.div_euclid(60);
        let days = minutes.div_euclid(1_440);
        let minute = minutes.rem_euclid(60) as u8;
        let hour = minutes.div_euclid(60).rem_euclid(24) as u8;
        let (year, month, day) = civil_from_days(days);
        let dow = (days + 4).rem_euclid(7) as u8;
        if year < 1970 {
            return false;
        }

        let dom_match = self.day_of_month.matches(day as u8);
        let dow_match = self.day_of_week.matches(dow);
        let day_match = match (self.day_of_month.wildcard, self.day_of_week.wildcard) {
            (true, true) => true,
            (true, false) => dow_match,
            (false, true) => dom_match,
            (false, false) => dom_match || dow_match,
        };

        self.minute.matches(minute)
            && self.hour.matches(hour)
            && self.month.matches(month as u8)
            && day_match
    }

    /// Finds the next matching minute strictly after `after_ms`.
    /// Search is bounded to eight calendar years to reject impossible specs.
    pub fn next_after_utc_ms(&self, after_ms: i64) -> Result<i64, CronError> {
        let minute_ms = 60_000i64;
        let base = after_ms
            .div_euclid(minute_ms)
            .checked_add(1)
            .and_then(|v| v.checked_mul(minute_ms))
            .ok_or(CronError::TimeOverflow)?;
        let max_minutes = 8usize * 366 * 24 * 60;
        let mut candidate = base;
        for _ in 0..max_minutes {
            if self.matches_utc_ms(candidate) {
                return Ok(candidate);
            }
            candidate = candidate
                .checked_add(minute_ms)
                .ok_or(CronError::TimeOverflow)?;
        }
        Err(CronError::InvalidField(
            "no matching time within search horizon".into(),
        ))
    }
}

impl Field {
    fn parse(input: &str, min: u8, max: u8) -> Result<Self, CronError> {
        let mut allowed = vec![false; usize::from(max - min + 1)];
        let wildcard = input == "*";
        for part in input.split(',') {
            let (base, step) = match part.split_once('/') {
                Some((base, step)) => {
                    let step = step
                        .parse::<u8>()
                        .map_err(|_| CronError::InvalidField(input.into()))?;
                    if step == 0 {
                        return Err(CronError::InvalidField(input.into()));
                    }
                    (base, step)
                }
                None => (part, 1),
            };
            let (start, end) = if base == "*" {
                (min, max)
            } else if let Some((start, end)) = base.split_once('-') {
                (
                    parse_value(start, min, max, input)?,
                    parse_value(end, min, max, input)?,
                )
            } else {
                let value = parse_value(base, min, max, input)?;
                (value, value)
            };
            if start > end {
                return Err(CronError::InvalidField(input.into()));
            }
            let mut value = start;
            loop {
                allowed[usize::from(value - min)] = true;
                let Some(next) = value.checked_add(step) else {
                    break;
                };
                if next > end {
                    break;
                }
                value = next;
            }
        }
        if !allowed.iter().any(|value| *value) {
            return Err(CronError::InvalidField(input.into()));
        }
        Ok(Self {
            min,
            max,
            allowed,
            wildcard,
        })
    }

    fn matches(&self, value: u8) -> bool {
        if value < self.min || value > self.max {
            return false;
        }
        self.allowed[usize::from(value - self.min)]
    }
}

fn parse_value(value: &str, min: u8, max: u8, original: &str) -> Result<u8, CronError> {
    let value = value
        .parse::<u8>()
        .map_err(|_| CronError::InvalidField(original.into()))?;
    if value < min || value > max {
        return Err(CronError::InvalidField(original.into()));
    }
    Ok(value)
}

// Howard Hinnant's civil-from-days algorithm. Input is days since Unix epoch.
fn civil_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 }.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096).div_euclid(365);
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2).div_euclid(153);
    let day = doy - (153 * mp + 2).div_euclid(5) + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_supports_wildcards_ranges_lists_and_steps() {
        let cron = CronExpression::parse("*/15 9-17 * 1,6 1-5").unwrap();
        assert!(cron.minute.matches(0));
        assert!(cron.minute.matches(45));
        assert!(!cron.minute.matches(46));
        assert!(cron.hour.matches(9));
        assert!(cron.hour.matches(17));
        assert!(cron.month.matches(1));
        assert!(cron.month.matches(6));
        assert!(!cron.month.matches(2));
    }

    #[test]
    fn next_after_finds_next_minute() {
        let cron = CronExpression::parse("*/5 * * * *").unwrap();
        assert_eq!(cron.next_after_utc_ms(0).unwrap(), 300_000);
        assert_eq!(cron.next_after_utc_ms(300_000).unwrap(), 600_000);
    }

    #[test]
    fn weekday_and_day_of_month_follow_standard_or_semantics() {
        // 1970-01-01 was Thursday (4). Either DOM=1 or DOW=4 matches.
        let cron = CronExpression::parse("0 0 1 * 5").unwrap();
        assert!(cron.matches_utc_ms(0));
    }

    #[test]
    fn invalid_specs_are_rejected() {
        assert!(CronExpression::parse("* * * *").is_err());
        assert!(CronExpression::parse("60 * * * *").is_err());
        assert!(CronExpression::parse("*/0 * * * *").is_err());
    }
}
