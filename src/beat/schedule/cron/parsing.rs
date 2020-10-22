use std::collections::HashSet;
use thiserror::Error;

use super::Ordinal;
use crate::error::BeatError;

#[derive(Error, Debug)]
#[error("Error")]
struct CronParsingError;

impl From<std::num::ParseIntError> for CronParsingError {
    fn from(_p: std::num::ParseIntError) -> CronParsingError {
        CronParsingError
    }
}

enum ParsedElement {
    Star,
    StarWithStep {
        step: Ordinal,
    },
    Number(Ordinal),
    Range {
        lower: Ordinal,
        upper: Ordinal,
    }, // inclusive
    RangeWithStep {
        lower: Ordinal,
        upper: Ordinal,
        step: Ordinal,
    },
}

pub fn parse_list(
    s: &str,
    lower_bound: Ordinal,
    upper_bound: Ordinal,
) -> Result<Vec<Ordinal>, BeatError> {
    let without_whitespace: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    let mut result = HashSet::new();
    for element in without_whitespace.split(',') {
        let parsed_element = parse_element_with_step(element);
        use ParsedElement::*;
        match parsed_element {
            Err(_) => return Err(BeatError::CronScheduleError(s.to_string())),
            Ok(Star) => {
                for i in lower_bound..=upper_bound {
                    result.insert(i);
                }
            }
            Ok(StarWithStep { step }) => {
                for i in (lower_bound..=upper_bound).step_by(step as usize) {
                    result.insert(i);
                }
            }
            Ok(Number(i)) => {
                result.insert(i);
            }
            Ok(Range { lower, upper }) => {
                for i in lower..=upper {
                    result.insert(i);
                }
            }
            Ok(RangeWithStep { lower, upper, step }) => {
                for i in (lower..=upper).step_by(step as usize) {
                    result.insert(i);
                }
            }
        }
    }

    let mut result: Vec<_> = result.iter().map(|n| *n).collect();
    result.sort_unstable();
    Ok(result)
}

fn parse_element(s: &str) -> Result<ParsedElement, CronParsingError> {
    use ParsedElement::*;

    if s == "*" {
        Ok(Star)
    } else {
        if let Some(i) = s.find('-') {
            let lower = s[0..i].parse()?;
            let upper = s[i + 1..].parse()?;
            Ok(Range { lower, upper })
        } else {
            let number = s.parse()?;
            Ok(Number(number))
        }
    }
}

fn parse_element_with_step(s: &str) -> Result<ParsedElement, CronParsingError> {
    use ParsedElement::*;
    if let Some(i) = s.find('/') {
        let element_without_step = parse_element(&s[0..i])?;
        let step = s[i + 1..].parse().map_err(|_| CronParsingError)?;
        match element_without_step {
            Star => Ok(StarWithStep { step }),
            Number(_) => panic!(),
            Range { lower, upper } => Ok(RangeWithStep { lower, upper, step }),
            _ => panic!(),
        }
    } else {
        parse_element(s)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{CronSchedule, Hours, Minutes, MonthDays, Months, WeekDays};
    use super::*;

    #[test]
    fn test_parse_list() -> Result<(), BeatError> {
        assert_eq!(parse_list("3", 2, 4)?, vec![3]);
        assert_eq!(parse_list("3-6/2", 2, 8)?, vec![3, 5]);
        assert_eq!(parse_list("*/3", 2, 8)?, vec![2, 5, 8]);
        assert_eq!(parse_list("*/3, 2, 7, 2-5/3", 2, 8)?, vec![2, 5, 7, 8]);
        assert!(parse_list(",", 2, 4).is_err());
        Ok(())
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
            Minutes::All => minutes == *&(1..=60).collect::<Vec<_>>(),
            Minutes::List(vec) => &minutes == vec,
        };
        let hours_equal = match &schedule.hours {
            Hours::All => hours == *&(0..=23).collect::<Vec<_>>(),
            Hours::List(vec) => &hours == vec,
        };
        let month_days_equal = match &schedule.month_days {
            MonthDays::All => month_days == *&(1..=31).collect::<Vec<_>>(),
            MonthDays::List(vec) => &month_days == vec,
        };
        let months_equal = match &schedule.months {
            Months::All => months == *&(1..=12).collect::<Vec<_>>(),
            Months::List(vec) => &months == vec,
        };
        let week_days_equal = match &schedule.week_days {
            WeekDays::All => week_days == *&(0..=6).collect::<Vec<_>>(),
            WeekDays::List(vec) => &week_days == vec,
        };

        minutes_equal && hours_equal && month_days_equal && months_equal && week_days_equal
    }

    #[test]
    fn test_parse_from_string() -> Result<(), BeatError> {
        let schedule = CronSchedule::from_string("2 12 8 1 *")?;
        assert!(cron_schedule_equal(
            &schedule,
            &vec![2],
            &vec![12],
            &vec![8],
            &vec![0, 1, 2, 3, 4, 5, 6],
            &vec![1]
        ));
        Ok(())
    }
}
