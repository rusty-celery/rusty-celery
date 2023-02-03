use std::collections::HashSet;

use super::{Ordinal, TimeUnitField};
use crate::error::ScheduleError;

pub struct CronParsingError;

pub enum Shorthand {
    Yearly,
    Monthly,
    Weekly,
    Daily,
    Hourly,
}

pub fn parse_shorthand(s: &str) -> Result<Shorthand, ScheduleError> {
    use Shorthand::*;
    match s.to_lowercase().as_str() {
        "@yearly" => Ok(Yearly),
        "@monthly" => Ok(Monthly),
        "@weekly" => Ok(Weekly),
        "@daily" => Ok(Daily),
        "@hourly" => Ok(Hourly),
        _ => Err(ScheduleError::CronScheduleError(format!(
            "'{s}' is not a valid shorthand for a cron schedule"
        ))),
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
    }, // inclusive
}

pub fn parse_longhand<T: TimeUnitField>(s: &str) -> Result<Vec<Ordinal>, ScheduleError> {
    let lower_bound = T::inclusive_min();
    let upper_bound = T::inclusive_max();
    let mut result = HashSet::new();

    for element in s.split(',') {
        let parsed_element = parse_element_with_step::<T>(element);
        use ParsedElement::*;
        match parsed_element {
            Err(_) => {
                let error_message = format!("'{}' is an invalid value for {}", s, T::name());
                return Err(ScheduleError::CronScheduleError(error_message));
            }
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

    let mut result: Vec<_> = result.iter().copied().collect();
    result.sort_unstable();
    Ok(result)
}

fn parse_element_with_step<T: TimeUnitField>(s: &str) -> Result<ParsedElement, CronParsingError> {
    use ParsedElement::*;
    if let Some(i) = s.find('/') {
        let element_without_step = parse_element::<T>(&s[0..i])?;
        let step = s[i + 1..].parse().map_err(|_| CronParsingError)?;
        match element_without_step {
            Star => Ok(StarWithStep { step }),
            Number(_) => Err(CronParsingError),
            Range { lower, upper } => Ok(RangeWithStep { lower, upper, step }),
            _ => panic!("parse_element returned an incorrect enum variant..."),
        }
    } else {
        parse_element::<T>(s)
    }
}

fn parse_element<T: TimeUnitField>(s: &str) -> Result<ParsedElement, CronParsingError> {
    use ParsedElement::*;

    if s == "*" {
        Ok(Star)
    } else if let Some(i) = s.find('-') {
        let lower = parse_ordinal::<T>(&s[0..i])?;
        let upper = parse_ordinal::<T>(&s[i + 1..])?;
        Ok(Range { lower, upper })
    } else {
        let number = parse_ordinal::<T>(s)?;
        Ok(Number(number))
    }
}

fn parse_ordinal<T: TimeUnitField>(s: &str) -> Result<Ordinal, CronParsingError> {
    if let Ok(ordinal) = s.parse() {
        Ok(ordinal)
    } else {
        T::ordinal_from_string(s)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Hours, Minutes, Months};
    use super::*;

    #[test]
    fn test_parse() -> Result<(), ScheduleError> {
        assert_eq!(parse_longhand::<Minutes>("3")?, vec![3]);
        assert_eq!(parse_longhand::<Minutes>("3-6/2")?, vec![3, 5]);
        assert_eq!(parse_longhand::<Months>("*/3")?, vec![1, 4, 7, 10]);
        assert_eq!(
            parse_longhand::<Hours>("*/3,2,7,2-5/3")?,
            vec![0, 2, 3, 5, 6, 7, 9, 12, 15, 18, 21]
        );
        assert!(parse_longhand::<Minutes>(",").is_err());
        Ok(())
    }
}
