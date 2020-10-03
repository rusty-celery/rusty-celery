use async_trait::async_trait;

use super::*;
use crate::error::TaskError;
use crate::task::{Request, Task, TaskOptions};

#[derive(Clone, Serialize, Deserialize)]
struct TestTaskParams {
    a: i32,
}

struct TestTask {
    request: Request<Self>,
    options: TaskOptions,
}

#[async_trait]
impl Task for TestTask {
    const NAME: &'static str = "test";
    const ARGS: &'static [&'static str] = &["a"];

    type Params = TestTaskParams;
    type Returns = ();

    fn from_request(request: Request<Self>, options: TaskOptions) -> Self {
        Self { request, options }
    }

    fn request(&self) -> &Request<Self> {
        &self.request
    }

    fn options(&self) -> &TaskOptions {
        &self.options
    }

    async fn run(&self, _params: Self::Params) -> Result<(), TaskError> {
        Ok(())
    }
}

const JSON: &str =
    "[[],{\"a\":4},{\"callbacks\":null,\"errbacks\":null,\"chain\":null,\"chord\":null}]";
#[test]
fn test_serialize_body() {
    let body = MessageBody::<TestTask>::new(TestTaskParams { a: 4 });
    let serialized = serde_json::to_string(&body).unwrap();
    assert_eq!(serialized, JSON);
}

#[test]
fn test_deserialize_body_with_args() {
    let message = Message {
        properties: MessageProperties {
            correlation_id: "aaa".into(),
            content_type: "application/json".into(),
            content_encoding: "utf-8".into(),
            reply_to: None,
        },
        headers: MessageHeaders {
            id: "aaa".into(),
            task: "TestTask".into(),
            ..Default::default()
        },
        raw_body: Vec::from(&JSON[..]),
    };
    let body = message.body::<TestTask>().unwrap();
    assert_eq!(body.1.a, 4);
}

const YAML: &str = "---\n- []\n- a: 4\n- callbacks: ~\n  errbacks: ~\n  chain: ~\n  chord: ~";

#[test]
fn test_yaml_serialize_body() {
    let body = MessageBody::<TestTask>::new(TestTaskParams { a: 4 });
    let serialized = serde_yaml::to_string(&body).unwrap();
    assert_eq!(serialized, YAML);
}

#[test]
fn test_yaml_deserialize_body_with_args() {
    let message = Message {
        properties: MessageProperties {
            correlation_id: "aaa".into(),
            content_type: "application/x-yaml".into(),
            content_encoding: "utf-8".into(),
            reply_to: None,
        },
        headers: MessageHeaders {
            id: "aaa".into(),
            task: "TestTask".into(),
            ..Default::default()
        },
        raw_body: Vec::from(&YAML[..]),
    };
    let body = message.body::<TestTask>().unwrap();
    assert_eq!(body.1.a, 4);
}

const PICKLE: &[u8] = b"\x80\x02(]}(X\x01\x00\x00\x00aJ\x04\x00\x00\x00u}(X\x09\x00\x00\x00callbacksNX\x08\x00\x00\x00errbacksNX\x05\x00\x00\x00chainNX\x05\x00\x00\x00chordNut.";
#[test]
fn test_pickle_serialize_body() {
    let body = MessageBody::<TestTask>::new(TestTaskParams { a: 4 });
    let serialized = serde_pickle::to_vec(&body, false).unwrap();
    // println!("{}", String::from_utf8(serialized.split_off(1)).unwrap());
    assert_eq!(serialized, PICKLE.to_vec());
}

#[test]
fn test_pickle_deserialize_body_with_args() {
    let message = Message {
        properties: MessageProperties {
            correlation_id: "aaa".into(),
            content_type: "application/x-python-serialize".into(),
            content_encoding: "utf-8".into(),
            reply_to: None,
        },
        headers: MessageHeaders {
            id: "aaa".into(),
            task: "TestTask".into(),
            ..Default::default()
        },
        raw_body: PICKLE.to_vec(),
    };
    let body = message.body::<TestTask>().unwrap();
    assert_eq!(body.1.a, 4);
}

const MSGPACK: &[u8] = &[147, 144, 145, 4, 148, 192, 192, 192, 192];
#[test]
fn test_msgpack_serialize_body() {
    let body = MessageBody::<TestTask>::new(TestTaskParams { a: 4 });
    let serialized = rmp_serde::to_vec(&body).unwrap();
    // println!("{:?}", serialized);
    assert_eq!(serialized, MSGPACK);
}

#[test]
fn test_msgpack_deserialize_body_with_args() {
    let message = Message {
        properties: MessageProperties {
            correlation_id: "aaa".into(),
            content_type: "application/x-msgpack".into(),
            content_encoding: "utf-8".into(),
            reply_to: None,
        },
        headers: MessageHeaders {
            id: "aaa".into(),
            task: "TestTask".into(),
            ..Default::default()
        },
        raw_body: MSGPACK.to_vec(),
    };
    let body = message.body::<TestTask>().unwrap();
    assert_eq!(body.1.a, 4);
}
