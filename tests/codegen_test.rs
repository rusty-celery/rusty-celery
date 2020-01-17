use celery::{task, Task};

#[task(name = "add")]
fn add(x: i32, y: i32) -> i32 {
    x + y
}

#[test]
fn test_add_name() {
    assert_eq!(add::NAME, "add");
}

#[test]
fn test_add_arg_names() {
    assert_eq!(add::ARGS, &["x", "y"]);
}

#[task]
fn add_auto_name(x: i32, y: i32) -> i32 {
    x + y
}

#[test]
fn test_auto_name() {
    assert_eq!(add_auto_name::NAME, "add_auto_name");
}

#[task(
    timeout = 2,
    max_retries = 3,
    min_retry_delay = 0,
    max_retry_delay = 60
)]
fn task_with_options() -> String {
    "it worked!".into()
}

#[test]
fn test_task_options() {
    let t = task_with_options();
    assert_eq!(t.timeout(), Some(2));
    assert_eq!(t.max_retries(), Some(3));
    assert_eq!(t.min_retry_delay(), Some(0));
    assert_eq!(t.max_retry_delay(), Some(60));
}
