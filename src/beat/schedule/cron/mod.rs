use std::time::SystemTime;

use super::Schedule;
use crate::error::CronScheduleError;

mod parsing;
mod time_units;
use parsing::{parse_longhand, parse_shorthand, CronParsingError, Shorthand};
use time_units::{Hours, Minutes, MonthDays, Months, TimeUnitField, WeekDays};

pub const MAX_YEAR: Ordinal = 2100;

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
    ) -> Result<CronSchedule, CronScheduleError> {
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

    pub fn from_string(schedule: &str) -> Result<CronSchedule, CronScheduleError> {
        if schedule.starts_with('@') {
            Self::from_shorthand(schedule)
        } else {
            Self::from_longhand(schedule)
        }
    }

    fn from_shorthand(schedule: &str) -> Result<CronSchedule, CronScheduleError> {
        use Shorthand::*;
        match parse_shorthand(schedule)? {
            Yearly => Ok(CronSchedule {
                minutes: Minutes::List(vec![0]),
                hours: Hours::List(vec![0]),
                month_days: MonthDays::List(vec![1]),
                months: Months::List(vec![1]),
                week_days: WeekDays::All,
            }),
            Monthly => Ok(CronSchedule {
                minutes: Minutes::List(vec![0]),
                hours: Hours::List(vec![0]),
                month_days: MonthDays::List(vec![1]),
                months: Months::All,
                week_days: WeekDays::All,
            }),
            Weekly => Ok(CronSchedule {
                minutes: Minutes::List(vec![0]),
                hours: Hours::List(vec![0]),
                month_days: MonthDays::All,
                months: Months::All,
                week_days: WeekDays::List(vec![1]),
            }),
            Daily => Ok(CronSchedule {
                minutes: Minutes::List(vec![0]),
                hours: Hours::List(vec![0]),
                month_days: MonthDays::All,
                months: Months::All,
                week_days: WeekDays::All,
            }),
            Hourly => Ok(CronSchedule {
                minutes: Minutes::List(vec![0]),
                hours: Hours::All,
                month_days: MonthDays::All,
                months: Months::All,
                week_days: WeekDays::All,
            }),
        }
    }

    fn from_longhand(schedule: &str) -> Result<CronSchedule, CronScheduleError> {
        let components: Vec<_> = schedule.split_whitespace().collect();
        if components.len() != 5 {
            Err(CronScheduleError(format!(
                "'{}' is not a valid cron schedule: invalid number of elements",
                schedule
            )))
        } else {
            let minutes = parse_longhand::<Minutes>(components[0])?;
            let hours = parse_longhand::<Hours>(components[1])?;
            let month_days = parse_longhand::<MonthDays>(components[2])?;
            let months = parse_longhand::<Months>(components[3])?;
            let week_days = parse_longhand::<WeekDays>(components[4])?;

            CronSchedule::new(minutes, hours, month_days, week_days, months)
        }
    }

    fn validate(
        minutes: &[Ordinal],
        hours: &[Ordinal],
        month_days: &[Ordinal],
        week_days: &[Ordinal],
        months: &[Ordinal],
    ) -> Result<(), CronScheduleError> {
        // TODO add checks for repeated values

        if minutes.is_empty() {
            return Err(CronScheduleError("TODO".to_string()));
        }
        if *minutes.last().unwrap() > 59 {
            return Err(CronScheduleError("TODO".to_string()));
        }

        if hours.is_empty() {
            return Err(CronScheduleError("TODO".to_string()));
        }
        if *hours.last().unwrap() > 23 {
            return Err(CronScheduleError("TODO".to_string()));
        }

        if month_days.is_empty() {
            return Err(CronScheduleError("TODO".to_string()));
        }
        if month_days[0] < 1 || *month_days.last().unwrap() > 31 {
            return Err(CronScheduleError("TODO".to_string()));
        }

        if week_days.is_empty() {
            return Err(CronScheduleError("TODO".to_string()));
        }
        if *week_days.last().unwrap() > 6 {
            return Err(CronScheduleError("TODO".to_string()));
        }

        if months.is_empty() {
            return Err(CronScheduleError("TODO".to_string()));
        }
        if months[0] < 1 || *months.last().unwrap() > 12 {
            return Err(CronScheduleError("TODO".to_string()));
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

    fn cron_schedule_equal(
        schedule: &CronSchedule,
        minutes: &[Ordinal],
        hours: &[Ordinal],
        month_days: &[Ordinal],
        week_days: &[Ordinal],
        months: &[Ordinal],
    ) -> bool {
        let minutes_equal = match &schedule.minutes {
            Minutes::All => minutes == (1..=60).collect::<Vec<_>>(),
            Minutes::List(vec) => &minutes == vec,
        };
        let hours_equal = match &schedule.hours {
            Hours::All => hours == (0..=23).collect::<Vec<_>>(),
            Hours::List(vec) => &hours == vec,
        };
        let month_days_equal = match &schedule.month_days {
            MonthDays::All => month_days == (1..=31).collect::<Vec<_>>(),
            MonthDays::List(vec) => &month_days == vec,
        };
        let months_equal = match &schedule.months {
            Months::All => months == (1..=12).collect::<Vec<_>>(),
            Months::List(vec) => &months == vec,
        };
        let week_days_equal = match &schedule.week_days {
            WeekDays::All => week_days == (0..=6).collect::<Vec<_>>(),
            WeekDays::List(vec) => &week_days == vec,
        };

        minutes_equal && hours_equal && month_days_equal && months_equal && week_days_equal
    }

    #[test]
    fn test_from_string() -> Result<(), CronScheduleError> {
        let schedule = CronSchedule::from_string("2 12 8 1 *")?;
        assert!(cron_schedule_equal(
            &schedule,
            &[2],
            &[12],
            &[8],
            &(0..=6).collect::<Vec<_>>(),
            &[1]
        ));

        let schedule = CronSchedule::from_string("@yearly")?;
        assert!(cron_schedule_equal(
            &schedule,
            &[0],
            &[0],
            &[1],
            &(0..=6).collect::<Vec<_>>(),
            &[1]
        ));
        let schedule = CronSchedule::from_string("@monthly")?;
        assert!(cron_schedule_equal(
            &schedule,
            &[0],
            &[0],
            &[1],
            &(0..=6).collect::<Vec<_>>(),
            &(1..=12).collect::<Vec<_>>()
        ));
        let schedule = CronSchedule::from_string("@weekly")?;
        assert!(cron_schedule_equal(
            &schedule,
            &[0],
            &[0],
            &(1..=31).collect::<Vec<_>>(),
            &[1],
            &(1..=12).collect::<Vec<_>>()
        ));
        let schedule = CronSchedule::from_string("@daily")?;
        assert!(cron_schedule_equal(
            &schedule,
            &[0],
            &[0],
            &(1..=31).collect::<Vec<_>>(),
            &(0..=6).collect::<Vec<_>>(),
            &(1..=12).collect::<Vec<_>>()
        ));
        let schedule = CronSchedule::from_string("@hourly")?;
        assert!(cron_schedule_equal(
            &schedule,
            &[0],
            &(0..=23).collect::<Vec<_>>(),
            &(1..=31).collect::<Vec<_>>(),
            &(0..=6).collect::<Vec<_>>(),
            &(1..=12).collect::<Vec<_>>()
        ));

        Ok(())
    }
}
