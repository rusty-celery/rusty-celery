use std::collections::HashSet;
use thiserror::Error;

use crate::error::BeatError;

pub type Ordinal = u32;
pub type SignedOrdinal = i32;

pub const MAX_YEAR: Ordinal = 2100; // TODO is this OK?

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

fn wrapped_gte(candidates: &[Ordinal], target: Ordinal) -> Ordinal {
    if candidates.is_empty() {
        target
    } else {
        if let Some(result) = gte_recursive_step(candidates, target) {
            result
        } else {
            candidates[0]
        }
    }
}

fn gte(candidates: &[Ordinal], target: Ordinal) -> Option<Ordinal> {
    if candidates.is_empty() {
        Some(target)
    } else if target > *candidates.last().unwrap() {
        None
    } else {
        gte_recursive_step(candidates, target)
    }
}

fn gte_recursive_step(candidates: &[Ordinal], target: Ordinal) -> Option<Ordinal> {
    assert!(!candidates.is_empty());

    let length = candidates.len();

    if length == 1 {
        if candidates[0] >= target {
            Some(candidates[0])
        } else {
            None
        }
    } else {
        let half_length = (length - 1) / 2;
        let middle_candidate = candidates[half_length];
        if middle_candidate == target {
            Some(middle_candidate)
        } else if middle_candidate < target {
            gte_recursive_step(&candidates[half_length + 1..], target)
        } else {
            gte_recursive_step(&candidates[0..half_length + 1], target)
        }
    }
}

pub fn subtract(subtrahend: Ordinal, minuend: Ordinal, base: Ordinal) -> (Ordinal, Ordinal) {
    let difference = subtrahend as SignedOrdinal - minuend as SignedOrdinal;
    if difference >= 0 {
        (difference as Ordinal, 0)
    } else {
        ((base as SignedOrdinal + difference) as Ordinal, 1)
    }
}

pub fn is_leap_year(year: Ordinal) -> bool {
    let by_four = year % 4 == 0;
    let by_hundred = year % 100 == 0;
    let by_four_hundred = year % 400 == 0;
    by_four && ((!by_hundred) || by_four_hundred)
}

pub fn days_in_month(month: Ordinal, year: Ordinal) -> Ordinal {
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

// #[cfg(test)]
// mod tests {
//     use super::super::CronSchedule;
//     use super::*;

//     #[test]
//     fn test_find() {
//         assert_eq!(wrapped_gte(&[1], 3), 1);
//         assert_eq!(wrapped_gte(&[5], 3), 5);
//         assert_eq!(wrapped_gte(&[3], 3), 3);
//         assert_eq!(wrapped_gte(&[1, 2, 4], 3), 4);
//         assert_eq!(wrapped_gte(&[5, 7, 9], 3), 5);
//         assert_eq!(wrapped_gte(&[5, 7, 9], 11), 5);
//     }

//     #[test]
//     fn test_parse_list() -> Result<(), BeatError> {
//         assert_eq!(parse_list("3", 2, 4)?, vec![3]);
//         assert_eq!(parse_list("3-6/2", 2, 8)?, vec![3, 5]);
//         assert_eq!(parse_list("*/3", 2, 8)?, vec![2, 5, 8]);
//         assert_eq!(parse_list("*/3, 2, 7, 2-5/3", 2, 8)?, vec![2, 5, 7, 8]);
//         assert!(parse_list(",", 2, 4).is_err());
//         Ok(())
//     }

//     fn cron_schedule_eq(
//         cron_schedule: &CronSchedule,
//         minutes: &[Ordinal],
//         hours: &[Ordinal],
//         days_of_month: &[Ordinal],
//         days_of_week: &[Ordinal],
//         months: &[Ordinal],
//     ) -> bool {
//         cron_schedule.minutes == minutes
//             && cron_schedule.hours == hours
//             && cron_schedule.days_of_month == days_of_month
//             && cron_schedule.days_of_week == days_of_week
//             && cron_schedule.months == months
//     }

//     #[test]
//     fn test_parse_from_string() -> Result<(), BeatError> {
//         assert!(cron_schedule_eq(
//             &CronSchedule::from_string("2 12 8 1 *")?,
//             &vec![2],
//             &vec![12],
//             &vec![8],
//             &vec![0, 1, 2, 3, 4, 5, 6],
//             &vec![1]
//         ));
//         Ok(())
//     }
// }

// trait TimeUnitField where Self: Sized {
//     fn open_range(&self, start: Ordinal) -> TimeUnitFieldIterator;
//     fn bounded_range(&self, start: Ordinal, stop: Ordinal) -> RangeInclusive<Ordinal>;
//     fn gte(&self, target: Ordinal) -> Option<Ordinal>;
//     fn inclusive_min(self) -> Ordinal;
//     fn inclusive_max(self) -> Ordinal;
// }

#[derive(Debug)]
pub enum Minutes {
    All,
    List(Vec<Ordinal>),
}

impl Minutes {
    pub fn from_vec(vec: Vec<Ordinal>) -> Self {
        if vec.len() == 60 {
            Minutes::All
        } else {
            Minutes::List(vec)
        }
    }
    pub fn open_range(&self, start: Ordinal) -> TimeUnitFieldIterator<'_> {
        use Minutes::*;
        match self {
            All => TimeUnitFieldIterator::from_range(start, Minutes::inclusive_max()),
            List(vec) => TimeUnitFieldIterator::from_vec(vec, start, Minutes::inclusive_max()),
        }
    }
    pub fn gte(&self, target: Ordinal) -> Option<Ordinal> {
        use Minutes::*;
        match self {
            All => {
                if target <= 59 {
                    Some(target)
                } else {
                    None
                }
            }
            List(vec) => gte(&vec, target),
        }
    }
    fn inclusive_min() -> Ordinal {
        0
    }
    fn inclusive_max() -> Ordinal {
        59
    }
}

#[derive(Debug)]
pub enum Hours {
    All,
    List(Vec<Ordinal>),
}

impl Hours {
    pub fn from_vec(vec: Vec<Ordinal>) -> Self {
        if vec.len() == 24 {
            Hours::All
        } else {
            Hours::List(vec)
        }
    }
    pub fn open_range(&self, start: Ordinal) -> TimeUnitFieldIterator<'_> {
        use Hours::*;
        match self {
            All => TimeUnitFieldIterator::from_range(start, Hours::inclusive_max()),
            List(vec) => TimeUnitFieldIterator::from_vec(vec, start, Hours::inclusive_max()),
        }
    }
    pub fn gte(&self, target: Ordinal) -> Option<Ordinal> {
        use Hours::*;
        match self {
            All => {
                if target <= 23 {
                    Some(target)
                } else {
                    None
                }
            }
            List(vec) => gte(&vec, target),
        }
    }
    fn inclusive_min() -> Ordinal {
        0
    }
    fn inclusive_max() -> Ordinal {
        23
    }
}

#[derive(Debug)]
pub enum DaysOfWeek {
    All,
    List(Vec<Ordinal>),
}

impl DaysOfWeek {
    pub fn from_vec(vec: Vec<Ordinal>) -> Self {
        if vec.len() == 7 {
            DaysOfWeek::All
        } else {
            DaysOfWeek::List(vec)
        }
    }
    pub fn contains(&self, target: Ordinal) -> bool {
        use DaysOfWeek::*;
        match self {
            All => target <= 6,
            List(vec) => vec.binary_search(&target).is_ok(),
        }
    }
    fn inclusive_min() -> Ordinal {
        0
    }
    fn inclusive_max() -> Ordinal {
        6
    }
}

#[derive(Debug)]
pub enum DaysOfMonth {
    All,
    List(Vec<Ordinal>),
}

impl DaysOfMonth {
    pub fn from_vec(vec: Vec<Ordinal>) -> Self {
        if vec.len() == 31 {
            DaysOfMonth::All
        } else {
            DaysOfMonth::List(vec)
        }
    }
    pub fn bounded_range(&self, start: Ordinal, stop: Ordinal) -> TimeUnitFieldIterator<'_> {
        use DaysOfMonth::*;
        match self {
            All => TimeUnitFieldIterator::from_range(start, stop),
            List(vec) => TimeUnitFieldIterator::from_vec(vec, start, stop),
        }
    }
    fn inclusive_min() -> Ordinal {
        1
    }
    fn inclusive_max() -> Ordinal {
        31
    }
}

#[derive(Debug)]
pub enum Months {
    All,
    List(Vec<Ordinal>),
}

impl Months {
    pub fn from_vec(vec: Vec<Ordinal>) -> Self {
        if vec.len() == 12 {
            Months::All
        } else {
            Months::List(vec)
        }
    }
    pub fn open_range(&self, start: Ordinal) -> TimeUnitFieldIterator<'_> {
        use Months::*;
        match self {
            All => TimeUnitFieldIterator::from_range(start, self.inclusive_max()),
            List(vec) => TimeUnitFieldIterator::from_vec(vec, start, self.inclusive_max()),
        }
    }
    fn inclusive_min(&self) -> Ordinal {
        1
    }
    fn inclusive_max(&self) -> Ordinal {
        12
    }
}

#[derive(Debug)]
pub enum TimeUnitFieldIterator<'a> {
    InclusiveRange {
        current: Ordinal,
        stop: Ordinal,
    },
    VecRange {
        vec: &'a Vec<Ordinal>,
        current: usize,
        stop: usize,
    },
}

impl<'a> TimeUnitFieldIterator<'a> {
    fn from_range(start: Ordinal, stop: Ordinal) -> Self {
        use TimeUnitFieldIterator::*;
        InclusiveRange {
            current: start,
            stop,
        }
    }

    fn from_vec(vec: &'a Vec<Ordinal>, lower_bound: Ordinal, upper_bound: Ordinal) -> Self {
        use TimeUnitFieldIterator::*;
        let mut vec_iter = vec.iter().enumerate().filter_map(|(i, x)| {
            if *x >= lower_bound && *x <= upper_bound {
                Some(i)
            } else {
                None
            }
        });
        if let Some(start) = vec_iter.next() {
            if let Some(stop) = vec_iter.last() {
                VecRange {
                    vec,
                    current: start,
                    stop,
                }
            } else {
                VecRange {
                    vec,
                    current: start,
                    stop: start,
                }
            }
        } else {
            VecRange {
                vec,
                current: 1,
                stop: 0,
            }
        }
    }
}

impl Iterator for TimeUnitFieldIterator<'_> {
    type Item = Ordinal;

    fn next(&mut self) -> Option<Self::Item> {
        use TimeUnitFieldIterator::*;
        match self {
            InclusiveRange { current, stop } => {
                if current <= stop {
                    let next = *current;
                    *current += 1;
                    Some(next)
                } else {
                    None
                }
            }
            VecRange { vec, current, stop } => {
                if current <= stop && *current < vec.len() {
                    let next = *current;
                    *current += 1;
                    Some(vec[next])
                } else {
                    None
                }
            }
        }
    }
}
