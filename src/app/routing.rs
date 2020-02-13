use globset::{Glob, GlobMatcher};

use crate::error::CeleryError;

/// A rule for routing tasks to a queue based on a glob pattern.
pub(super) struct Rule {
    pub(super) pattern: GlobMatcher,
    pub(super) queue: String,
}

impl Rule {
    pub(super) fn new(pattern: &str, queue: &str) -> Result<Self, CeleryError> {
        Ok(Self {
            pattern: Glob::new(pattern)?.compile_matcher(),
            queue: queue.into(),
        })
    }

    pub(super) fn is_match(&self, task_name: &str) -> bool {
        self.pattern.is_match(task_name)
    }
}

pub(super) fn route<'a>(task_name: &'a str, rules: &'a [Rule]) -> Option<&'a str> {
    for rule in rules {
        if rule.is_match(task_name) {
            return Some(&rule.queue);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_route() {
        let rules = vec![
            Rule::new("tasks.backend.*", "backend").unwrap(),
            Rule::new("tasks.ml.*", "ml").unwrap(),
            Rule::new("tasks.*", "celery").unwrap(),
        ];

        assert_eq!(route("tasks.ml.predict", &rules[..]), Some("ml"));
    }
}
