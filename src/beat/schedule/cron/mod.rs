use std::time::SystemTime;

use super::Schedule;
use crate::error::BeatError;

mod parsing;
mod time_units;
use parsing::parse_list;
use time_units::{Hours, Minutes, MonthDays, Months, WeekDays};

pub const MAX_YEAR: Ordinal = 2100; // TODO is this OK?

type Ordinal = u32;

#[derive(Debug)]
pub struct CronSchedule {
    minutes: Minutes,
    hours: Hours,
    month_days: MonthDays,
    week_days: WeekDays,
    months: Months,
}

impl Schedule for CronSchedule {
    fn next_call_at(&self, _last_run_at: Option<SystemTime>) -> Option<SystemTime> {
        let now = chrono::Utc::now(); // TODO support different time zones
        self.next(now).map(SystemTime::from)
    }
}

impl CronSchedule {
    pub fn new(
        mut minutes: Vec<Ordinal>,
        mut hours: Vec<Ordinal>,
        mut month_days: Vec<Ordinal>,
        mut week_days: Vec<Ordinal>,
        mut months: Vec<Ordinal>,
    ) -> Result<CronSchedule, BeatError> {
        minutes.sort_unstable();
        hours.sort_unstable();
        month_days.sort_unstable();
        week_days.sort_unstable();
        months.sort_unstable();

        CronSchedule::validate(&minutes, &hours, &month_days, &week_days, &months)?;

        Ok(CronSchedule {
            minutes: Minutes::from_vec(minutes),
            hours: Hours::from_vec(hours),
            month_days: MonthDays::from_vec(month_days),
            week_days: WeekDays::from_vec(week_days),
            months: Months::from_vec(months),
        })
    }

    pub fn from_string(schedule: &str) -> Result<CronSchedule, BeatError> {
        let components: Vec<_> = schedule.split(' ').collect();
        assert_eq!(components.len(), 5);
        let minutes = parse_list(components[0], 0, 59)?;
        let hours = parse_list(components[1], 0, 23)?;
        let month_days = parse_list(components[2], 1, 31)?;
        let months = parse_list(components[3], 1, 12)?;
        let week_days = parse_list(components[4], 0, 6)?;

        CronSchedule::new(minutes, hours, month_days, week_days, months)
    }

    fn validate(
        minutes: &[Ordinal],
        hours: &[Ordinal],
        month_days: &[Ordinal],
        week_days: &[Ordinal],
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

        if month_days.is_empty() {
            return Err(BeatError::CronScheduleError("TODO".to_string()));
        }
        if month_days[0] < 1 || *month_days.last().unwrap() > 31 {
            return Err(BeatError::CronScheduleError("TODO".to_string()));
        }

        if week_days.is_empty() {
            return Err(BeatError::CronScheduleError("TODO".to_string()));
        }
        if *week_days.last().unwrap() > 6 {
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
        let current_month_day = now.day();
        let current_month = now.month();
        let current_year = now.year() as Ordinal;

        let mut overflow = false;
        for year in current_year..MAX_YEAR {
            let month_start = if overflow { 1 } else { current_month };
            for month in self.months.open_range(month_start) {
                if month > current_month {
                    overflow = true;
                }
                let month_day_start = if overflow { 1 } else { current_month_day };
                let num_days_in_month = days_in_month(month, year);
                'day_loop: for month_day in self
                    .month_days
                    .bounded_range(month_day_start, num_days_in_month)
                {
                    if month_day > current_month_day {
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
                                .ymd(year as i32, month, month_day)
                                .and_hms_opt(hour, minute, 0)
                            {
                                // Check that the day of week is correct
                                if !self
                                    .week_days
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

fn is_leap_year(year: Ordinal) -> bool {
    let by_four = year % 4 == 0;
    let by_hundred = year % 100 == 0;
    let by_four_hundred = year % 400 == 0;
    by_four && ((!by_hundred) || by_four_hundred)
}

fn days_in_month(month: Ordinal, year: Ordinal) -> Ordinal {
    let is_leap_year = is_leap_year(year);
    match month {
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year => 29,
        2 => 28,
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        x => panic!(
            "{} is not a valid value for a month (it must be between 1 and 12)",
            x
        ),
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
