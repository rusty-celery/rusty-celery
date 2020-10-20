/// This module contains the definition of application-provided schedules.
///
/// This structs have not changed a lot compared to Python: in Python there are three
/// different types of schedules: `schedule` (corresponding to [`RegularSchedule`]),
/// `crontab` (not implemented yet), `solar` (not implemented yet).
use std::time::{Duration, SystemTime};

use crate::error::BeatError;

mod cron;
use cron::{Ordinal, SignedOrdinal};

/// The trait that all schedules implement.
///
/// For now, we only support regular schedules.
pub trait Schedule {
    fn next_call_at(&self, last_run_at: Option<SystemTime>) -> Option<SystemTime>;
}

/// When using this schedule, tasks are executed at regular intervals.
pub struct RegularSchedule {
    interval: Duration,
}

impl RegularSchedule {
    pub fn new(interval: Duration) -> RegularSchedule {
        RegularSchedule { interval }
    }
}

impl Schedule for RegularSchedule {
    fn next_call_at(&self, last_run_at: Option<SystemTime>) -> Option<SystemTime> {
        match last_run_at {
            Some(last_run_at) => Some(
                last_run_at
                    .checked_add(self.interval)
                    .expect("Invalid SystemTime encountered"),
            ),
            None => Some(SystemTime::now()),
        }
    }
}

pub struct CronSchedule {
    minutes: Vec<Ordinal>,
    hours: Vec<Ordinal>,
    days_of_month: Vec<Ordinal>,
    days_of_week: Vec<Ordinal>,
    months: Vec<Ordinal>,
}

impl Schedule for CronSchedule {
    fn next_call_at(&self, _last_run_at: Option<SystemTime>) -> Option<SystemTime> {
        let now = chrono::Utc::now(); // TODO support different time zones
        let seconds_to_wait = self.seconds_to_next_call(now);
        Some(SystemTime::now() + Duration::from_secs(seconds_to_wait as u64))
    }
}

impl CronSchedule {
    pub fn new(
        mut minutes: Vec<Ordinal>,
        mut hours: Vec<Ordinal>,
        mut days_of_month: Vec<Ordinal>,
        mut days_of_week: Vec<Ordinal>,
        mut months: Vec<Ordinal>,
    ) -> Result<CronSchedule, BeatError> {
        minutes.sort_unstable();
        hours.sort_unstable();
        days_of_month.sort_unstable();
        days_of_week.sort_unstable();
        months.sort_unstable();

        CronSchedule::validate(&minutes, &hours, &days_of_month, &days_of_week, &months)?;

        if minutes.len() == 60 {
            minutes = vec![];
        }
        if hours.len() == 24 {
            hours = vec![];
        }
        if days_of_month.len() == 31 {
            days_of_month = vec![];
        }
        if days_of_week.len() == 7 {
            days_of_week = vec![];
        }
        if months.len() == 12 {
            months = vec![];
        }

        Ok(CronSchedule {
            minutes,
            hours,
            days_of_month,
            days_of_week,
            months,
        })
    }

    pub fn from_string(schedule: &str) -> Result<CronSchedule, BeatError> {
        let components: Vec<_> = schedule.split(' ').collect();
        assert_eq!(components.len(), 5);
        let minutes = cron::parse_list(components[0], 0, 59)?;
        let hours = cron::parse_list(components[1], 0, 23)?;
        let days_of_month = cron::parse_list(components[2], 1, 31)?;
        let months = cron::parse_list(components[3], 1, 12)?;
        let days_of_week = cron::parse_list(components[4], 0, 6)?;

        CronSchedule::new(minutes, hours, days_of_month, days_of_week, months)
    }

    fn validate(
        minutes: &[Ordinal],
        hours: &[Ordinal],
        days_of_month: &[Ordinal],
        days_of_week: &[Ordinal],
        months: &[Ordinal],
    ) -> Result<(), BeatError> {
        // TODO add checks for repeated values

        if minutes.is_empty() {
            return Err(BeatError::CronScheduleError("TODO".to_string()));
        }
        if *minutes.last().unwrap() > 59 {
            return Err(BeatError::CronScheduleError("TODO".to_string()));
        }

        if hours.is_empty() {
            return Err(BeatError::CronScheduleError("TODO".to_string()));
        }
        if *hours.last().unwrap() > 23 {
            return Err(BeatError::CronScheduleError("TODO".to_string()));
        }

        if days_of_month.is_empty() {
            return Err(BeatError::CronScheduleError("TODO".to_string()));
        }
        if days_of_month[0] < 1 || *days_of_month.last().unwrap() > 31 {
            return Err(BeatError::CronScheduleError("TODO".to_string()));
        }

        if days_of_week.is_empty() {
            return Err(BeatError::CronScheduleError("TODO".to_string()));
        }
        if *days_of_week.last().unwrap() > 6 {
            return Err(BeatError::CronScheduleError("TODO".to_string()));
        }

        if months.is_empty() {
            return Err(BeatError::CronScheduleError("TODO".to_string()));
        }
        if months[0] < 1 || *months.last().unwrap() > 12 {
            return Err(BeatError::CronScheduleError("TODO".to_string()));
        }

        Ok(())
    }

    fn minutes_to_wait(&self, current_minute: Ordinal, carry: u32) -> (Ordinal, u32) {
        let target_minute = current_minute + carry;
        let next_minute = cron::wrapped_gte(&self.minutes, target_minute);
        cron::subtract(next_minute, target_minute, 60)
    }

    fn hours_to_wait(&self, current_hour: Ordinal, carry: u32) -> (Ordinal, u32) {
        let target_hour = current_hour + carry;
        let next_hour = cron::wrapped_gte(&self.hours, target_hour);
        dbg!(target_hour);
        dbg!(next_hour);
        cron::subtract(next_hour, target_hour, 24)
    }

    fn days_to_wait_by_day_of_week(
        &self,
        current_day_of_week: Ordinal,
        carry: u32,
        current_day_of_month: Ordinal,
        days_in_current_month: Ordinal,
    ) -> (Ordinal, u32) {
        let target_day_of_week = current_day_of_week + carry;
        let next_day_of_week = cron::wrapped_gte(&self.days_of_week, target_day_of_week);
        let (days_to_wait, _) = cron::subtract(next_day_of_week, target_day_of_week, 7);
        let carry = if current_day_of_month + days_to_wait > days_in_current_month {
            1
        } else {
            0
        };
        (days_to_wait, carry)
    }

    fn days_to_wait_by_day_of_month(
        &self,
        current_day_of_month: Ordinal,
        carry: u32,
        current_month: u32,
        current_year: u32,
    ) -> (Ordinal, u32) {
        let days_in_current_month = cron::days_in_month(current_month, current_year);
        let target_day_of_month = current_day_of_month + carry;
        let mut next_day_of_month = cron::wrapped_gte(&self.days_of_month, target_day_of_month);
        if next_day_of_month > days_in_current_month {
            if self.days_of_month.is_empty() {
                next_day_of_month = 0;
            } else {
                next_day_of_month = self.days_of_month[0];
            }
        }
        // Cannot use subtract because the base is not a constant
        let difference = next_day_of_month as SignedOrdinal - target_day_of_month as SignedOrdinal;
        if difference >= 0 {
            (difference as Ordinal, 0)
        } else {
            let mut carry = 0;
            let mut carried_difference = difference;
            while carried_difference < 0 {
                carry += 1;
                carried_difference +=
                    cron::days_in_month(current_month + carry, current_year) as SignedOrdinal;
            }
            (carried_difference as Ordinal, carry)
        }
    }

    fn days_to_wait_by_month(
        &self,
        current_month: Ordinal,
        carry: u32,
        current_year: Ordinal,
    ) -> Ordinal {
        let target_month = current_month + carry;
        let next_month = cron::wrapped_gte(&self.months, target_month);
        let (months_to_wait, _) = cron::subtract(next_month, target_month, 12);

        (0..months_to_wait)
            .map(|i| {
                let year = if current_month + i < 12 {
                    current_year
                } else {
                    current_year + 1
                };
                cron::days_in_month(i % 12, year)
            })
            .sum()
    }

    fn seconds_to_next_call<Tz>(&self, now: chrono::DateTime<Tz>) -> u32
    where
        Tz: chrono::TimeZone,
    {
        let mut seconds_to_wait = 0;
        let mut carry = 0;

        use chrono::{Datelike, Timelike};

        let current_second = now.second();
        let current_minute = now.minute();
        let current_hour = now.hour();
        let current_day_of_month = now.day();
        let current_day_of_week = now.weekday().num_days_from_sunday();
        let current_month = now.month();
        let current_year = now.year() as Ordinal;
        let days_in_current_month = cron::days_in_month(current_month, current_year);

        if current_second > 0 {
            seconds_to_wait += 60 - current_second;
            carry = 1;
        }

        let (minutes_to_wait, carry) = self.minutes_to_wait(current_minute, carry);
        dbg!("{}", minutes_to_wait);
        seconds_to_wait += minutes_to_wait * 60;

        let (hours_to_wait, carry) = self.hours_to_wait(current_hour, carry);
        dbg!("{}", hours_to_wait);
        seconds_to_wait += hours_to_wait * 3600;

        let (days_to_wait, carry) = if self.days_of_month.is_empty() {
            self.days_to_wait_by_day_of_week(
                current_day_of_week,
                carry,
                current_day_of_month,
                days_in_current_month,
            )
        } else if self.days_of_week.is_empty() {
            self.days_to_wait_by_day_of_month(
                current_day_of_month,
                carry,
                current_month,
                current_year,
            )
        } else {
            let (days_to_wait_by_week, carry_by_week) = self.days_to_wait_by_day_of_week(
                current_day_of_week,
                carry,
                current_day_of_month,
                days_in_current_month,
            );
            let (days_to_wait_by_month, carry_by_month) = self.days_to_wait_by_day_of_month(
                current_day_of_month,
                carry,
                current_month,
                current_year,
            );
            if days_to_wait_by_week >= days_to_wait_by_month {
                (days_to_wait_by_week, carry_by_week)
            } else {
                (days_to_wait_by_month, carry_by_month)
            }
        };
        dbg!("{}", days_to_wait);
        seconds_to_wait += days_to_wait * 3600 * 24;

        seconds_to_wait +=
            self.days_to_wait_by_month(current_month, carry, current_year) * 3600 * 24;
        dbg!("{}", self.days_to_wait_by_month(current_month, carry, current_year));

        seconds_to_wait
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cron() {
        // let date =
        //     chrono::DateTime::parse_from_str("2020-10-19 20:30:00 +0000", "%Y-%m-%d %H:%M:%S %z")
        //         .unwrap();
        // let cron_schedule = CronSchedule::from_string("* * * * *").unwrap();
        // assert_eq!(0, cron_schedule.seconds_to_next_call(date));

        // let date =
        //     chrono::DateTime::parse_from_str("2020-10-19 20:30:00 +0000", "%Y-%m-%d %H:%M:%S %z")
        //         .unwrap();
        // let cron_schedule = CronSchedule::from_string("31 20 * * *").unwrap();
        // assert_eq!(60, cron_schedule.seconds_to_next_call(date));

        // let date =
        //     chrono::DateTime::parse_from_str("2020-10-19 20:30:00 +0000", "%Y-%m-%d %H:%M:%S %z")
        //         .unwrap();
        // let cron_schedule = CronSchedule::from_string("31 14 4 11 *").unwrap();
        // assert_eq!(1274460, cron_schedule.seconds_to_next_call(date));

        let date =
            chrono::DateTime::parse_from_str("2020-10-19 20:29:23 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap();
        let cron_schedule = CronSchedule::from_string("*/5 9-18 1 * 6,0").unwrap();
        assert_eq!(1274460, cron_schedule.seconds_to_next_call(date));
    }
}
