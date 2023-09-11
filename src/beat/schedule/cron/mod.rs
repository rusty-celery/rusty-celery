//! This module contains the implementation of cron schedules.
//! The implementation is inspired by the
//! [cron crate](https://crates.io/crates/cron).

use chrono::{offset::Utc, TimeZone};
use std::time::SystemTime;

use super::Schedule;
use crate::error::ScheduleError;

mod parsing;
mod time_units;
use parsing::{parse_longhand, parse_shorthand, CronParsingError, Shorthand};
use time_units::{Hours, Minutes, MonthDays, Months, TimeUnitField, WeekDays};

/// The maximum year supported by a `CronSchedule`.
pub const MAX_YEAR: Ordinal = 2100;

/// The type used to represent a temporal element (minutes, hours...).
type Ordinal = u32;

/// A schedule that can be used to execute tasks using Celery's
/// [crontab](https://docs.celeryproject.org/en/stable/reference/celery.schedules.html#celery.schedules.crontab)
/// syntax.
///
/// # Examples
///
/// ```
/// // Schedule a task every 5 minutes from Monday to Friday:
/// celery::beat::CronSchedule::from_string("*/5 * * * mon-fri");
///
/// // Schedule a task each minute from 8 am to 5 pm on the first day
/// // of every month but only if it is Sunday:
/// celery::beat::CronSchedule::from_string("* 8-17 1 * sun");
///
/// // Execute every minute in march with a custom time zone:
/// let time_zone = chrono::offset::FixedOffset::east(3600);
/// celery::beat::CronSchedule::from_string_with_time_zone("* * * mar *", time_zone);
/// ```
///
/// A schedule can also be defined using vectors with the required
/// candidates:
/// ```
/// celery::beat::CronSchedule::new(
///     vec![15,30,45,59,0],
///     vec![0,23],
///     vec![1,2,3],
///     vec![1,2,3,4,12],
///     (1..=6).collect(),
/// );
/// ```
#[derive(Debug)]
pub struct CronSchedule<Z>
where
    Z: TimeZone,
{
    minutes: Minutes,
    hours: Hours,
    month_days: MonthDays,
    months: Months,
    week_days: WeekDays,
    time_zone: Z,
}

impl<Z> Schedule for CronSchedule<Z>
where
    Z: TimeZone,
{
    fn next_call_at(&self, _last_run_at: Option<SystemTime>) -> Option<SystemTime> {
        let now = self.time_zone.from_utc_datetime(&Utc::now().naive_utc());
        self.next(now).map(SystemTime::from)
    }
}

impl CronSchedule<Utc> {
    /// Create a new cron schedule which can be used to run a task
    /// in the specified minutes/hours/month days/week days/months.
    /// This schedule will use the UTC time zone.
    ///
    /// No vector should be empty and each argument must be in the range
    /// of valid values for its respective time unit.
    pub fn new(
        minutes: Vec<Ordinal>,
        hours: Vec<Ordinal>,
        month_days: Vec<Ordinal>,
        months: Vec<Ordinal>,
        week_days: Vec<Ordinal>,
    ) -> Result<Self, ScheduleError> {
        Self::new_with_time_zone(minutes, hours, month_days, months, week_days, Utc)
    }

    /// Create a cron schedule from a *cron* string. This schedule will use the
    /// UTC time zone.
    ///
    /// The string must be a space-separated list of five elements,
    /// representing *minutes*, *hours*, *month days*, *months* and *week days*
    /// (in this order). Each element can be:
    /// - a number in the correct range for the given time unit: e.g. `3`
    /// - a range: e.g. `2-5` which corresponds to 2,3,4,5
    /// - a range with a step: e.g. `1-6/3` which corresponds to 1,4
    /// - a wildcard: i.e. `*` which corresponds to all elements for the given time unit
    /// - a wildcard with a step: e.g. `*/4` which corresponds to one every four elements
    /// - a comma-separated list (without spaces) where each element is one
    ///   of the previous ones: e.g. `8,2-4,1-5/2` which corresponds to 1,2,3,4,5,8
    ///
    /// Months and week days can also be represented using the first three letters instead
    /// of numbers (e.g, `mon`, `thu`, `may`, `oct`...).
    ///
    /// As an alternative, a shorthand representation can be used. The following options
    /// are available:
    /// - `@yearly`: at 0:00 on the first of January each year
    /// - `@monthly`: at 0:00 at the beginning of each month
    /// - `@weekly`: at 0:00 on Monday each week
    /// - `@daily`: at 0:00 each day
    /// - `@hourly`: each hour at 00
    pub fn from_string(schedule: &str) -> Result<Self, ScheduleError> {
        Self::from_string_with_time_zone(schedule, Utc)
    }
}

impl<Z> CronSchedule<Z>
where
    Z: TimeZone,
{
    /// Create a new cron schedule which can be used to run a task
    /// in the specified minutes/hours/month days/week days/months.
    /// This schedule will use the given time zone.
    ///
    /// No vector should be empty and each argument must be in the range
    /// of valid values for its respective time unit.
    pub fn new_with_time_zone(
        mut minutes: Vec<Ordinal>,
        mut hours: Vec<Ordinal>,
        mut month_days: Vec<Ordinal>,
        mut months: Vec<Ordinal>,
        mut week_days: Vec<Ordinal>,
        time_zone: Z,
    ) -> Result<Self, ScheduleError> {
        minutes.sort_unstable();
        minutes.dedup();
        hours.sort_unstable();
        hours.dedup();
        month_days.sort_unstable();
        month_days.dedup();
        months.sort_unstable();
        months.dedup();
        week_days.sort_unstable();
        week_days.dedup();

        Self::validate(&minutes, &hours, &month_days, &months, &week_days)?;

        Ok(Self {
            minutes: Minutes::from_vec(minutes),
            hours: Hours::from_vec(hours),
            month_days: MonthDays::from_vec(month_days),
            months: Months::from_vec(months),
            week_days: WeekDays::from_vec(week_days),
            time_zone,
        })
    }

    /// Create a cron schedule from a *cron* string. This schedule will use the
    /// given time zone.
    ///
    /// The string must be a space-separated list of five elements,
    /// representing *minutes*, *hours*, *month days*, *months* and *week days*
    /// (in this order). Each element can be:
    /// - a number in the correct range for the given time unit: e.g. `3`
    /// - a range: e.g. `2-5` which corresponds to 2,3,4,5
    /// - a range with a step: e.g. `1-6/3` which corresponds to 1,4
    /// - a wildcard: i.e. `*` which corresponds to all elements for the given time unit
    /// - a wildcard with a step: e.g. `*/4` which corresponds to one every four elements
    /// - a comma-separated list (without spaces) where each element is one
    ///   of the previous ones: e.g. `8,2-4,1-5/2` which corresponds to 1,2,3,4,5,8
    ///
    /// Months and week days can also be represented using the first three letters instead
    /// of numbers (e.g, `mon`, `thu`, `may`, `oct`...).
    ///
    /// As an alternative, a shorthand representation can be used. The following options
    /// are available:
    /// - `@yearly`: at 0:00 on the first of January each year
    /// - `@monthly`: at 0:00 at the beginning of each month
    /// - `@weekly`: at 0:00 on Monday each week
    /// - `@daily`: at 0:00 each day
    /// - `@hourly`: each hour at 00
    pub fn from_string_with_time_zone(schedule: &str, time_zone: Z) -> Result<Self, ScheduleError> {
        if schedule.starts_with('@') {
            Self::from_shorthand(schedule, time_zone)
        } else {
            Self::from_longhand(schedule, time_zone)
        }
    }

    /// Check that the given vectors are in the correct range for each time unit
    /// and are not empty.
    fn validate(
        minutes: &[Ordinal],
        hours: &[Ordinal],
        month_days: &[Ordinal],
        months: &[Ordinal],
        week_days: &[Ordinal],
    ) -> Result<(), ScheduleError> {
        use ScheduleError::CronScheduleError;

        if minutes.is_empty() {
            return Err(CronScheduleError("Minutes were not set".to_string()));
        }
        if *minutes.first().unwrap() < Minutes::inclusive_min() {
            return Err(CronScheduleError(format!(
                "Minutes cannot be less than {}",
                Minutes::inclusive_min()
            )));
        }
        if *minutes.last().unwrap() > Minutes::inclusive_max() {
            return Err(CronScheduleError(format!(
                "Minutes cannot be more than {}",
                Minutes::inclusive_max()
            )));
        }

        if hours.is_empty() {
            return Err(CronScheduleError("Hours were not set".to_string()));
        }
        if *hours.first().unwrap() < Hours::inclusive_min() {
            return Err(CronScheduleError(format!(
                "Hours cannot be less than {}",
                Hours::inclusive_min()
            )));
        }
        if *hours.last().unwrap() > Hours::inclusive_max() {
            return Err(CronScheduleError(format!(
                "Hours cannot be more than {}",
                Hours::inclusive_max()
            )));
        }

        if month_days.is_empty() {
            return Err(CronScheduleError("Month days were not set".to_string()));
        }
        if *month_days.first().unwrap() < MonthDays::inclusive_min() {
            return Err(CronScheduleError(format!(
                "Month days cannot be less than {}",
                MonthDays::inclusive_min()
            )));
        }
        if *month_days.last().unwrap() > MonthDays::inclusive_max() {
            return Err(CronScheduleError(format!(
                "Month days cannot be more than {}",
                MonthDays::inclusive_max()
            )));
        }

        if months.is_empty() {
            return Err(CronScheduleError("Months were not set".to_string()));
        }
        if *months.first().unwrap() < Months::inclusive_min() {
            return Err(CronScheduleError(format!(
                "Months cannot be less than {}",
                Months::inclusive_min()
            )));
        }
        if *months.last().unwrap() > Months::inclusive_max() {
            return Err(CronScheduleError(format!(
                "Months cannot be more than {}",
                Months::inclusive_max()
            )));
        }

        if week_days.is_empty() {
            return Err(CronScheduleError("Week days were not set".to_string()));
        }
        if *week_days.first().unwrap() < WeekDays::inclusive_min() {
            return Err(CronScheduleError(format!(
                "Week days cannot be less than {}",
                WeekDays::inclusive_min()
            )));
        }
        if *week_days.last().unwrap() > WeekDays::inclusive_max() {
            return Err(CronScheduleError(format!(
                "Week days cannot be more than {}",
                WeekDays::inclusive_max()
            )));
        }

        Ok(())
    }

    fn from_shorthand(schedule: &str, time_zone: Z) -> Result<Self, ScheduleError> {
        use Shorthand::*;
        match parse_shorthand(schedule)? {
            Yearly => Ok(Self {
                minutes: Minutes::List(vec![0]),
                hours: Hours::List(vec![0]),
                month_days: MonthDays::List(vec![1]),
                week_days: WeekDays::All,
                months: Months::List(vec![1]),
                time_zone,
            }),
            Monthly => Ok(Self {
                minutes: Minutes::List(vec![0]),
                hours: Hours::List(vec![0]),
                month_days: MonthDays::List(vec![1]),
                week_days: WeekDays::All,
                months: Months::All,
                time_zone,
            }),
            Weekly => Ok(Self {
                minutes: Minutes::List(vec![0]),
                hours: Hours::List(vec![0]),
                month_days: MonthDays::All,
                week_days: WeekDays::List(vec![1]),
                months: Months::All,
                time_zone,
            }),
            Daily => Ok(Self {
                minutes: Minutes::List(vec![0]),
                hours: Hours::List(vec![0]),
                month_days: MonthDays::All,
                week_days: WeekDays::All,
                months: Months::All,
                time_zone,
            }),
            Hourly => Ok(Self {
                minutes: Minutes::List(vec![0]),
                hours: Hours::All,
                month_days: MonthDays::All,
                week_days: WeekDays::All,
                months: Months::All,
                time_zone,
            }),
        }
    }

    fn from_longhand(schedule: &str, time_zone: Z) -> Result<Self, ScheduleError> {
        let components: Vec<_> = schedule.split_whitespace().collect();
        if components.len() != 5 {
            Err(ScheduleError::CronScheduleError(format!(
                "'{schedule}' is not a valid cron schedule: invalid number of elements"
            )))
        } else {
            let minutes = parse_longhand::<Minutes>(components[0])?;
            let hours = parse_longhand::<Hours>(components[1])?;
            let month_days = parse_longhand::<MonthDays>(components[2])?;
            let months = parse_longhand::<Months>(components[3])?;
            let week_days = parse_longhand::<WeekDays>(components[4])?;

            CronSchedule::new_with_time_zone(
                minutes, hours, month_days, months, week_days, time_zone,
            )
        }
    }

    /// Compute the next time a task should run according to this schedule
    /// using `now` as starting point.
    ///
    /// Note that `Tz` in theory can be a time zone different from the time zone `Z`
    /// associated with this instance of `CronSchedule`: this method will work regardless.
    /// In practice, however, `Tz` and `Z` are always the same.
    ///
    /// ## Algorithm description
    ///
    /// The time units form the following hierarchy: year -> month -> days of month -> hour -> minute.
    /// The algorithm loops over them in this order
    /// and for every candidate checks if it corresponds to the correct week day
    /// and is valid according to the given time zone. Using graph terminology, we are exploring
    /// the tree of possible solutions using a depth-first search.
    ///
    /// For each unit in the hierarchy we only pick candidates which are valid
    /// according to our cron schedule.
    ///
    /// ### Meaning of the `overflow` variable
    ///
    /// The starting point of the search is equal to
    /// `(current year, current month, current month day, current hour, current minute)`.
    ///
    /// At first we require that the candidate for each level is equal to the corresponding
    /// "current value". If no candidate is found for that value we have to try larger values.
    /// When this happens all candidates at later levels are not required to be greater than the
    /// "current value" anymore.
    ///
    /// The `overflow` variable is used to model this part of the algorithm.
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
        assert!(current_year <= MAX_YEAR);

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
                            if let chrono::offset::LocalResult::Single(candidate) = timezone
                                .with_ymd_and_hms(year as i32, month, month_day, hour, minute, 0)
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
    use chrono::{DateTime, NaiveDateTime};

    fn make_utc_date(s: &str) -> DateTime<Utc> {
        DateTime::<Utc>::from_naive_utc_and_offset(
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S %z").unwrap(),
            Utc,
        )
    }

    #[test]
    fn test_cron_next() {
        let date = make_utc_date("2020-10-19 20:30:00 +0000");
        let cron_schedule = CronSchedule::from_string("* * * * *").unwrap();
        let expected_date = make_utc_date("2020-10-19 20:31:00 +0000");
        assert_eq!(Some(expected_date), cron_schedule.next(date));

        let date = make_utc_date("2020-10-19 20:30:00 +0000");
        let cron_schedule = CronSchedule::from_string("31 20 * * *").unwrap();
        let expected_date = make_utc_date("2020-10-19 20:31:00 +0000");
        assert_eq!(Some(expected_date), cron_schedule.next(date));

        let date = make_utc_date("2020-10-19 20:30:00 +0000");
        let cron_schedule = CronSchedule::from_string("31 14 4 11 *").unwrap();
        let expected_date = make_utc_date("2020-11-04 14:31:00 +0000");
        assert_eq!(Some(expected_date), cron_schedule.next(date));

        let date = make_utc_date("2020-10-19 20:29:23 +0000");
        let cron_schedule = CronSchedule::from_string("*/5 9-18 1 * 6,0").unwrap();
        let expected_date = make_utc_date("2020-11-01 09:00:00 +0000");
        assert_eq!(Some(expected_date), cron_schedule.next(date));

        let date = make_utc_date("2020-10-19 20:29:23 +0000");
        let cron_schedule = CronSchedule::from_string("3 12 29-31 1-6 2-4").unwrap();
        let expected_date = make_utc_date("2021-03-30 12:03:00 +0000");
        assert_eq!(Some(expected_date), cron_schedule.next(date));

        let date = make_utc_date("2020-10-19 20:29:23 +0000");
        let cron_schedule = CronSchedule::from_string("* * 30 2 *").unwrap();
        assert_eq!(None, cron_schedule.next(date));
    }

    #[test]
    fn test_cron_next_with_date_time() {
        let date =
            chrono::DateTime::parse_from_str("2020-10-19 20:29:23 +0112", "%Y-%m-%d %H:%M:%S %z")
                .unwrap();
        let time_zone = chrono::offset::FixedOffset::east_opt(3600 + 600 + 120).unwrap();
        let cron_schedule =
            CronSchedule::from_string_with_time_zone("3 12 29-31 1-6 2-4", time_zone).unwrap();
        let expected_date =
            chrono::DateTime::parse_from_str("2021-03-30 12:03:00 +0112", "%Y-%m-%d %H:%M:%S %z")
                .unwrap();
        assert_eq!(Some(expected_date), cron_schedule.next(date));
    }

    fn cron_schedule_equal<Z: TimeZone>(
        schedule: &CronSchedule<Z>,
        minutes: &[Ordinal],
        hours: &[Ordinal],
        month_days: &[Ordinal],
        months: &[Ordinal],
        week_days: &[Ordinal],
    ) -> bool {
        let minutes_equal = match &schedule.minutes {
            Minutes::All => minutes == (1..=60).collect::<Vec<_>>(),
            Minutes::List(vec) => minutes == vec,
        };
        let hours_equal = match &schedule.hours {
            Hours::All => hours == (0..=23).collect::<Vec<_>>(),
            Hours::List(vec) => hours == vec,
        };
        let month_days_equal = match &schedule.month_days {
            MonthDays::All => month_days == (1..=31).collect::<Vec<_>>(),
            MonthDays::List(vec) => month_days == vec,
        };
        let months_equal = match &schedule.months {
            Months::All => months == (1..=12).collect::<Vec<_>>(),
            Months::List(vec) => months == vec,
        };
        let week_days_equal = match &schedule.week_days {
            WeekDays::All => week_days == (0..=6).collect::<Vec<_>>(),
            WeekDays::List(vec) => week_days == vec,
        };

        minutes_equal && hours_equal && month_days_equal && months_equal && week_days_equal
    }

    #[test]
    fn test_from_string() -> Result<(), ScheduleError> {
        let schedule = CronSchedule::from_string("2 12 8 1 *")?;
        assert!(cron_schedule_equal(
            &schedule,
            &[2],
            &[12],
            &[8],
            &[1],
            &(0..=6).collect::<Vec<_>>(),
        ));

        let schedule = CronSchedule::from_string("@yearly")?;
        assert!(cron_schedule_equal(
            &schedule,
            &[0],
            &[0],
            &[1],
            &[1],
            &(0..=6).collect::<Vec<_>>(),
        ));
        let schedule = CronSchedule::from_string("@monthly")?;
        assert!(cron_schedule_equal(
            &schedule,
            &[0],
            &[0],
            &[1],
            &(1..=12).collect::<Vec<_>>(),
            &(0..=6).collect::<Vec<_>>(),
        ));
        let schedule = CronSchedule::from_string("@weekly")?;
        assert!(cron_schedule_equal(
            &schedule,
            &[0],
            &[0],
            &(1..=31).collect::<Vec<_>>(),
            &(1..=12).collect::<Vec<_>>(),
            &[1],
        ));
        let schedule = CronSchedule::from_string("@daily")?;
        assert!(cron_schedule_equal(
            &schedule,
            &[0],
            &[0],
            &(1..=31).collect::<Vec<_>>(),
            &(1..=12).collect::<Vec<_>>(),
            &(0..=6).collect::<Vec<_>>(),
        ));
        let schedule = CronSchedule::from_string("@hourly")?;
        assert!(cron_schedule_equal(
            &schedule,
            &[0],
            &(0..=23).collect::<Vec<_>>(),
            &(1..=31).collect::<Vec<_>>(),
            &(1..=12).collect::<Vec<_>>(),
            &(0..=6).collect::<Vec<_>>(),
        ));

        Ok(())
    }
}
