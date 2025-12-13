use std::hint::black_box;

use bytes::BytesMut;
use criterion::{criterion_group, criterion_main, Criterion};

use monodb_common::protocol::{
    ProtocolCodec, Request, Response, ExecutionResult,
};
use monodb_common::Value;

fn make_request() -> Request {
    Request::Execute {
        query: "get from users where name = $1".into(),
        params: vec![Value::String("John Doe".to_string())],
        snapshot_timestamp: Some(1_725_000_000),
        user_id: Some("test".into()),
    }
}

fn make_response() -> Response {
    Response::Success {
        result: vec![
            ExecutionResult::Ok {
                data: vec![
                    Value::from(1u64),
                    Value::from("alice"),
                ],
                time: 12,
                commit_timestamp: Some(1_725_000_010),
                time_elapsed: Some(3),
                row_count: Some(1),
            }
        ],
    }
}

fn bench_encode_request(c: &mut Criterion) {
    let req = make_request();

    c.bench_function("protocol::encode_request", |b| {
        b.iter(|| {
            let bytes = ProtocolCodec::encode_request(black_box(&req)).unwrap();
            black_box(bytes);
        })
    });
}

fn bench_decode_request(c: &mut Criterion) {
    let req = make_request();
    let encoded = ProtocolCodec::encode_request(&req).unwrap();

    c.bench_function("protocol::decode_request", |b| {
        b.iter(|| {
            let mut buf = BytesMut::from(encoded.as_ref());
            let decoded = ProtocolCodec::decode_request(&mut buf).unwrap();
            black_box(decoded);
        })
    });
}

fn bench_encode_response(c: &mut Criterion) {
    let resp = make_response();

    c.bench_function("protocol::encode_response", |b| {
        b.iter(|| {
            let bytes = ProtocolCodec::encode_response(black_box(&resp)).unwrap();
            black_box(bytes);
        })
    });
}

fn bench_decode_response(c: &mut Criterion) {
    let resp = make_response();
    let encoded = ProtocolCodec::encode_response(&resp).unwrap();

    c.bench_function("protocol::decode_response", |b| {
        b.iter(|| {
            let mut buf = BytesMut::from(encoded.as_ref());
            let decoded = ProtocolCodec::decode_response(&mut buf).unwrap();
            black_box(decoded);
        })
    });
}

criterion_group!(
    protocol_codec,
    bench_encode_request,
    bench_decode_request,
    bench_encode_response,
    bench_decode_response,
);

criterion_main!(protocol_codec);