/// This module contains the definition of application-provided schedules.
///
/// This structs have not changed a lot compared to Python: in Python there are three
/// different types of schedules: `schedule` (corresponding to [`RegularSchedule`]),
/// `crontab` (not implemented yet), `solar` (not implemented yet).
use std::time::{Duration, SystemTime};

use crate::error::BeatError;

mod cron;
use cron::{DaysOfMonth, DaysOfWeek, Hours, Minutes, Months, Ordinal};

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

#[derive(Debug)]
pub struct CronSchedule {
    minutes: Minutes,
    hours: Hours,
    days_of_month: DaysOfMonth,
    days_of_week: DaysOfWeek,
    months: Months,
}

impl Schedule for CronSchedule {
    fn next_call_at(&self, _last_run_at: Option<SystemTime>) -> Option<SystemTime> {
        let now = chrono::Utc::now(); // TODO support different time zones
        if let Some(next) = self.next(now) {
            let duration = next - now;
            Some(SystemTime::now() + duration.to_std().expect("Is this safe? TODO"))
        } else {
            None
        }
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

        Ok(CronSchedule {
            minutes: Minutes::from_vec(minutes),
            hours: Hours::from_vec(hours),
            days_of_month: DaysOfMonth::from_vec(days_of_month),
            days_of_week: DaysOfWeek::from_vec(days_of_week),
            months: Months::from_vec(months),
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

    fn next<Tz>(&self, now: chrono::DateTime<Tz>) -> Option<chrono::DateTime<Tz>>
    where
        Tz: chrono::TimeZone,
    {
        use chrono::{Datelike, Timelike};

        let current_minute = now.minute();
        let current_hour = now.hour();
        let current_day_of_month = now.day();
        let current_month = now.month();
        let current_year = now.year() as Ordinal;

        let mut overflow = false;
        for year in current_year..cron::MAX_YEAR {
            let month_start = if overflow { 1 } else { current_month };
            for month in self.months.open_range(month_start) {
                if month > current_month {
                    overflow = true;
                }
                let day_of_month_start = if overflow { 1 } else { current_day_of_month };
                let num_days_in_month = cron::days_in_month(month, year);
                'day_loop: for day_of_month in self
                    .days_of_month
                    .bounded_range(day_of_month_start, num_days_in_month)
                {
                    if day_of_month > current_day_of_month {
                        overflow = true;
                    }
                    let hour_target = if overflow { 0 } else { current_hour };
                    for hour in self.hours.open_range(hour_target) {
                        if hour > current_hour {
                            overflow = true;
                        }
                        let minute_target = if overflow { 0 } else { current_minute + 1 };
                        for minute in self.minutes.open_range(minute_target) {
                            // Check that date is real (time zones are complicated...)
                            let timezone = now.timezone();
                            if let Some(candidate) = timezone
                                .ymd(year as i32, month, day_of_month)
                                .and_hms_opt(hour, minute, 0)
                            {
                                // Check that the day of week is correct
                                if !self
                                    .days_of_week
                                    .contains(candidate.weekday().num_days_from_sunday())
                                {
                                    // It makes no sense trying different hours and
                                    // minutes in the same day
                                    continue 'day_loop;
                                }

                                return Some(candidate);
                            }
                        }
                        overflow = true;
                    }
                    overflow = true;
                }
                overflow = true;
            }
            overflow = true;
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cron_next() {
        let date =
            chrono::DateTime::parse_from_str("2020-10-19 20:30:00 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap();
        let cron_schedule = CronSchedule::from_string("* * * * *").unwrap();
        let expected_date =
            chrono::DateTime::parse_from_str("2020-10-19 20:31:00 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap();
        assert_eq!(Some(expected_date), cron_schedule.next(date));

        let date =
            chrono::DateTime::parse_from_str("2020-10-19 20:30:00 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap();
        let cron_schedule = CronSchedule::from_string("31 20 * * *").unwrap();
        let expected_date =
            chrono::DateTime::parse_from_str("2020-10-19 20:31:00 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap();
        assert_eq!(Some(expected_date), cron_schedule.next(date));

        let date =
            chrono::DateTime::parse_from_str("2020-10-19 20:30:00 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap();
        let cron_schedule = CronSchedule::from_string("31 14 4 11 *").unwrap();
        let expected_date =
            chrono::DateTime::parse_from_str("2020-11-04 14:31:00 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap();
        assert_eq!(Some(expected_date), cron_schedule.next(date));

        let date =
            chrono::DateTime::parse_from_str("2020-10-19 20:29:23 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap();
        let cron_schedule = CronSchedule::from_string("*/5 9-18 1 * 6,0").unwrap();
        let expected_date =
            chrono::DateTime::parse_from_str("2020-11-01 09:00:00 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap();
        assert_eq!(Some(expected_date), cron_schedule.next(date));

        let date =
            chrono::DateTime::parse_from_str("2020-10-19 20:29:23 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap();
        let cron_schedule = CronSchedule::from_string("3 12 29-31 1-6 2-4").unwrap();
        let expected_date =
            chrono::DateTime::parse_from_str("2021-03-30 12:03:00 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap();
        assert_eq!(Some(expected_date), cron_schedule.next(date));

        let date =
            chrono::DateTime::parse_from_str("2020-10-19 20:29:23 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap();
        let cron_schedule = CronSchedule::from_string("* * 30 2 *").unwrap();
        assert_eq!(None, cron_schedule.next(date));
    }
}
