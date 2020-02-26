use celery::task::Task;

#[celery::task(name = "add")]
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

#[celery::task]
fn add_auto_name(x: i32, y: i32) -> i32 {
    x + y
}

#[test]
fn test_auto_name() {
    assert_eq!(add_auto_name::NAME, "add_auto_name");
}

#[celery::task(
    timeout = 2,
    max_retries = 3,
    min_retry_delay = 0,
    max_retry_delay = 60,
    acks_late = true
)]
fn task_with_options() -> String {
    "it worked!".into()
}

#[test]
fn test_task_options() {
    let t = task_with_options::new();
    assert_eq!(t.timeout(), Some(2));
    assert_eq!(t.max_retries(), Some(3));
    assert_eq!(t.min_retry_delay(), Some(0));
    assert_eq!(t.max_retry_delay(), Some(60));
    assert_eq!(t.acks_late(), Some(true));
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
fn task_with_strings(s1: String, s2: String) -> String {
    format!("{}, {}", s1, s2)
}

#[test]
fn test_task_with_strings() {
    let s = task_with_strings::s("hi".into(), "there".into());
    let t = task_with_strings::new();
    let result = futures::executor::block_on(t.run(s)).unwrap();
    assert_eq!(result, "hi, there");
}
