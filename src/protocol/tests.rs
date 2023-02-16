use async_trait::async_trait;

use super::*;
use crate::error::TaskError;
use crate::task::{Request, Task, TaskOptions};
use chrono::{DateTime, SecondsFormat, Utc};
use std::time::SystemTime;

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
        raw_body: Vec::from(JSON),
    };
    let body = message.body::<TestTask>().unwrap();
    assert_eq!(body.1.a, 4);
}

const YAML: &str =
    "- []\n- a: 4\n- callbacks: null\n  errbacks: null\n  chain: null\n  chord: null\n";

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
        raw_body: Vec::from(YAML),
    };
    let body = message.body::<TestTask>().unwrap();
    assert_eq!(body.1.a, 4);
}

const PICKLE: &[u8] = b"\x80\x03(]}(X\x01\x00\x00\x00aJ\x04\x00\x00\x00u}(X\x09\x00\x00\x00callbacksNX\x08\x00\x00\x00errbacksNX\x05\x00\x00\x00chainNX\x05\x00\x00\x00chordNut.";
#[test]
fn test_pickle_serialize_body() {
    let body = MessageBody::<TestTask>::new(TestTaskParams { a: 4 });
    let serialized = serde_pickle::to_vec(&body, serde_pickle::SerOptions::new()).unwrap();
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

#[test]
/// Tests message serialization.
fn test_serialization() {
    let now = DateTime::<Utc>::from(SystemTime::now());
    // HACK: round this to milliseconds because that will happen during conversion
    // from message -> delivery.
    let now_str = now.to_rfc3339_opts(SecondsFormat::Millis, false);
    let now = DateTime::<Utc>::from(DateTime::parse_from_rfc3339(&now_str).unwrap());

    let message = Message {
        properties: MessageProperties {
            correlation_id: "aaa".into(),
            content_type: "application/json".into(),
            content_encoding: "utf-8".into(),
            reply_to: Some("bbb".into()),
        },
        headers: MessageHeaders {
            id: "aaa".into(),
            task: "add".into(),
            lang: Some("rust".into()),
            root_id: Some("aaa".into()),
            parent_id: Some("000".into()),
            group: Some("A".into()),
            meth: Some("method_name".into()),
            shadow: Some("add-these".into()),
            eta: Some(now),
            expires: Some(now),
            retries: Some(1),
            timelimit: (Some(30), Some(60)),
            argsrepr: Some("(1)".into()),
            kwargsrepr: Some("{'y': 2}".into()),
            origin: Some("gen123@piper".into()),
        },
        raw_body: Vec::from(JSON),
    };
    let ser_msg_result = message.json_serialized();
    assert!(ser_msg_result.is_ok());
    let ser_msg = ser_msg_result.unwrap();
    let ser_msg_json: serde_json::Value = serde_json::from_slice(&ser_msg[..]).unwrap();
    assert_eq!(ser_msg_json["content-encoding"], String::from("utf-8"));
    assert_eq!(
        ser_msg_json["content-type"],
        String::from("application/json")
    );
    assert_eq!(
        ser_msg_json["properties"]["correlation_id"],
        String::from("aaa")
    );
    assert_eq!(ser_msg_json["properties"]["reply_to"], String::from("bbb"));
    assert_ne!(ser_msg_json["properties"]["delivery_tag"], "");
    assert_eq!(
        ser_msg_json["properties"]["body_encoding"],
        String::from("base64")
    );
    assert_eq!(ser_msg_json["headers"]["id"], String::from("aaa"));
    assert_eq!(ser_msg_json["headers"]["task"], String::from("add"));
    assert_eq!(ser_msg_json["headers"]["lang"], String::from("rust"));
    assert_eq!(ser_msg_json["headers"]["root_id"], String::from("aaa"));
    assert_eq!(ser_msg_json["headers"]["parent_id"], String::from("000"));
    assert_eq!(ser_msg_json["headers"]["group"], String::from("A"));
    assert_eq!(ser_msg_json["headers"]["meth"], String::from("method_name"));
    assert_eq!(ser_msg_json["headers"]["shadow"], String::from("add-these"));
    assert_eq!(ser_msg_json["headers"]["retries"], 1);
    assert_eq!(ser_msg_json["headers"]["eta"], now_str);
    assert_eq!(ser_msg_json["headers"]["expires"], now_str);
    assert_eq!(ser_msg_json["headers"]["timelimit"][0], 30);
    assert_eq!(ser_msg_json["headers"]["timelimit"][1], 60);
    assert_eq!(ser_msg_json["headers"]["argsrepr"], "(1)");
    assert_eq!(ser_msg_json["headers"]["kwargsrepr"], "{'y': 2}");
    assert_eq!(ser_msg_json["headers"]["origin"], "gen123@piper");
    let body = ENGINE
        .decode(ser_msg_json["body"].as_str().unwrap())
        .unwrap();
    assert_eq!(body.len(), 73);
    assert_eq!(&body, JSON.as_bytes());
}
