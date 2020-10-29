use super::{CronParsingError, Ordinal};

#[derive(Debug)]
pub enum Minutes {
    All,
    List(Vec<Ordinal>),
}

pub trait TimeUnitField {
    fn name() -> &'static str;
    fn inclusive_min() -> Ordinal;
    fn inclusive_max() -> Ordinal;
    fn ordinal_from_string(s: &str) -> Result<Ordinal, CronParsingError>;
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
}

impl TimeUnitField for Minutes {
    fn name() -> &'static str {
        "minutes"
    }
    fn inclusive_min() -> Ordinal {
        0
    }
    fn inclusive_max() -> Ordinal {
        59
    }
    fn ordinal_from_string(_s: &str) -> Result<Ordinal, CronParsingError> {
        Err(CronParsingError)
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
}

impl TimeUnitField for Hours {
    fn name() -> &'static str {
        "hours"
    }
    fn inclusive_min() -> Ordinal {
        0
    }
    fn inclusive_max() -> Ordinal {
        23
    }
    fn ordinal_from_string(_s: &str) -> Result<Ordinal, CronParsingError> {
        Err(CronParsingError)
    }
}

#[derive(Debug)]
pub enum WeekDays {
    All,
    List(Vec<Ordinal>),
}

impl WeekDays {
    pub fn from_vec(vec: Vec<Ordinal>) -> Self {
        if vec.len() == 7 {
            WeekDays::All
        } else {
            WeekDays::List(vec)
        }
    }
    pub fn contains(&self, target: Ordinal) -> bool {
        use WeekDays::*;
        match self {
            All => target <= 6,
            List(vec) => vec.binary_search(&target).is_ok(),
        }
    }
}

impl TimeUnitField for WeekDays {
    fn name() -> &'static str {
        "week days"
    }
    fn inclusive_min() -> Ordinal {
        0
    }
    fn inclusive_max() -> Ordinal {
        6
    }
    fn ordinal_from_string(s: &str) -> Result<Ordinal, CronParsingError> {
        let result = match s.to_lowercase().as_str() {
            "sun" => 0,
            "mon" => 1,
            "tue" => 2,
            "wed" => 3,
            "thu" => 4,
            "fri" => 5,
            "sat" => 6,
            _ => return Err(CronParsingError),
        };
        Ok(result)
    }
}

#[derive(Debug)]
pub enum MonthDays {
    All,
    List(Vec<Ordinal>),
}

impl MonthDays {
    pub fn from_vec(vec: Vec<Ordinal>) -> Self {
        if vec.len() == 31 {
            MonthDays::All
        } else {
            MonthDays::List(vec)
        }
    }
    pub fn bounded_range(&self, start: Ordinal, stop: Ordinal) -> TimeUnitFieldIterator<'_> {
        use MonthDays::*;
        match self {
            All => TimeUnitFieldIterator::from_range(start, stop),
            List(vec) => TimeUnitFieldIterator::from_vec(vec, start, stop),
        }
    }
}

impl TimeUnitField for MonthDays {
    fn name() -> &'static str {
        "month days"
    }
    fn inclusive_min() -> Ordinal {
        1
    }
    fn inclusive_max() -> Ordinal {
        31
    }
    fn ordinal_from_string(_s: &str) -> Result<Ordinal, CronParsingError> {
        Err(CronParsingError)
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
            All => TimeUnitFieldIterator::from_range(start, Months::inclusive_max()),
            List(vec) => TimeUnitFieldIterator::from_vec(vec, start, Months::inclusive_max()),
        }
    }
}

impl TimeUnitField for Months {
    fn name() -> &'static str {
        "months"
    }
    fn inclusive_min() -> Ordinal {
        1
    }
    fn inclusive_max() -> Ordinal {
        12
    }
    fn ordinal_from_string(s: &str) -> Result<Ordinal, CronParsingError> {
        let result = match s.to_lowercase().as_str() {
            "jan" => 1,
            "feb" => 2,
            "mar" => 3,
            "apr" => 4,
            "may" => 5,
            "jun" => 6,
            "jul" => 7,
            "aug" => 8,
            "sep" => 9,
            "oct" => 10,
            "nov" => 11,
            "dec" => 12,
            _ => return Err(CronParsingError),
        };
        Ok(result)
    }
}

#[derive(Debug)]
pub enum TimeUnitFieldIterator<'a> {
    InclusiveRange {
        current: Ordinal,
        stop: Ordinal,
    },
    VecRange {
        vec: &'a [Ordinal],
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

    fn from_vec(vec: &'a [Ordinal], lower_bound: Ordinal, upper_bound: Ordinal) -> Self {
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
