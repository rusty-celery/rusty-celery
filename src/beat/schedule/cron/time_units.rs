use super::Ordinal;

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
    fn inclusive_min() -> Ordinal {
        0
    }
    fn inclusive_max() -> Ordinal {
        23
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
    fn inclusive_min() -> Ordinal {
        0
    }
    fn inclusive_max() -> Ordinal {
        6
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
