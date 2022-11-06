use std::collections::HashMap;

use celery::error::TaskError;
use celery::task::{Task, TaskResult};

#[celery::task(name = "add")]
fn add(x: i32, y: i32) -> TaskResult<i32> {
    Ok(x + y)
}

#[test]
fn test_add_name() {
    assert_eq!(add::NAME, "add");
}

#[test]
fn test_add_arg_names() {
    assert_eq!(add::ARGS, &["x", "y"]);
}

#[celery::task]
fn add_auto_name(x: i32, y: i32) -> TaskResult<i32> {
    Ok(x + y)
}

#[test]
fn test_auto_name() {
    assert_eq!(add_auto_name::NAME, "add_auto_name");
}

#[celery::task(
    time_limit = 2,
    hard_time_limit = 3,
    max_retries = 3,
    min_retry_delay = 0,
    max_retry_delay = 60,
    retry_for_unexpected = false,
    acks_late = true
)]
fn task_with_options() -> TaskResult<String> {
    Ok("it worked!".into())
}

#[test]
fn test_task_options() {
    assert_eq!(task_with_options::DEFAULTS.time_limit, Some(2));
    assert_eq!(task_with_options::DEFAULTS.hard_time_limit, Some(3));
    assert_eq!(task_with_options::DEFAULTS.max_retries, Some(3));
    assert_eq!(task_with_options::DEFAULTS.min_retry_delay, Some(0));
    assert_eq!(task_with_options::DEFAULTS.max_retry_delay, Some(60));
    assert_eq!(
        task_with_options::DEFAULTS.retry_for_unexpected,
        Some(false)
    );
    assert_eq!(task_with_options::DEFAULTS.acks_late, Some(true));
}

#[celery::task(bind = true)]
fn bound_task(t: &Self) -> TaskResult<Option<u32>> {
    t.retry_with_countdown(2)?;
    Ok(t.time_limit())
}

#[celery::task(bind = true)]
fn bound_task_with_other_params(t: &Self, default_time_limit: u32) -> TaskResult<u32> {
    Ok(t.time_limit().unwrap_or(default_time_limit))
}

// This didn't work before since Task::run took a reference to self
// instead of consuming self, so it was like
//
// fn run(&mut self) {
//     let s1 = self.s1;
//     let s2 = self.s2;
//     ...
// }
//
// which doesn't compile since String doesn't implement Copy.
//
// After changing the signature of `run` to consume `self` this now works.
#[celery::task]
fn task_with_strings(s1: String, s2: String) -> TaskResult<String> {
    Ok(format!("{}, {}", s1, s2))
}

async fn task_on_failure<T: Task>(task: &T, _err: &TaskError) {
    println!("Ahhhhh task {}[{}] failed!", task.name(), task.request().id);
}

async fn task_on_success<T: Task>(task: &T, _ret: &T::Returns) {
    println!(
        "Woooooo task {}[{}] succeeded!",
        task.name(),
        task.request().id
    );
}

#[celery::task(on_failure = task_on_failure, on_success = task_on_success)]
fn task_with_callbacks() {
    println!("Yeup yeup yeup");
}

#[celery::task]
fn inferred_return_type() {
    println!("Yeeeup");
}

#[cfg(any(test, feature = "extra_content_types"))]
use celery::protocol::MessageContentType::MsgPack;
#[celery::task(content_type = MsgPack)]
fn custom_content_type() {
    println!()
}

#[celery::task(context = "context")]
fn greet(context: HashMap<String, serde_json::Value>, name: String) -> TaskResult<String> {
    let greet = context.get("greet").unwrap().as_str().unwrap();
    Ok(format!("{} {}!", greet, name))
}
