// use anyhow::{Result, bail};
// use monodb_client::Client;
// use monodb_common::protocol::{ExecutionResult, Response};

// #[tokio::main]
// pub async fn main() -> Result<()> {
//     let client = Client::connect("localhost:7899").await;

//     let table_code = r#"
// make table users
//     as relational
//     fields
//         id int primary key unique
//         first_name text
//         last_name text
//         email text unique

// make table testing
//     as document

// make table sessions
//     as keyspace
//     persistence "memory"
// "#;

//     match client {
//         Ok(client) => {
//             let pool = client.pool().await;
//             let connection = pool.get().await;

//             match connection {
//                 Ok(mut conn) => {
//                     let _ = conn.execute(table_code.to_string()).await;

//                     let response = conn.list_tables().await;

//                     match response {
//                         Ok(resp) => match resp {
//                             Response::Success { result } => {
//                                 let result = result.get(0).unwrap();

//                                 match result {
//                                     ExecutionResult::Ok { data, .. } => {
//                                         println!("Data: {data}");
//                                     }
//                                     _ => bail!("Received unexpected ExecutionResult"),
//                                 }
//                             }
//                             _ => bail!("Received unexpected response type"),
//                         },
//                         Err(e) => {
//                             bail!(e)
//                         }
//                     }
//                 }
//                 Err(e) => {
//                     bail!(e)
//                 }
//             }
//         }
//         Err(e) => {
//             bail!(e)
//         }
//     }
//     Ok(())
// }

use bytes::BytesMut;
use monodb_common::{
    Value,
    protocol::{ProtocolCodec, Request},
};

fn example_requests() -> Vec<Request> {
    vec![
        Request::Connect {
            protocol_version: 1,
            auth_token: None,
        },
        Request::Connect {
            protocol_version: 1,
            auth_token: Some("example_auth_token".into()),
        },
        Request::Connect {
            protocol_version: 2,
            auth_token: None,
        },
        Request::Execute {
            query: "get from users".to_string(),
            params: Vec::new(),
            snapshot_timestamp: None,
            user_id: None,
        },
        Request::Execute {
            query: "get from users where\n\tfirst_name = $1".to_string(),
            params: vec![Value::String("Jacob".into())],
            snapshot_timestamp: None,
            user_id: None,
        },
        Request::Execute {
            query: "get from users".to_string(),
            params: Vec::new(),
            snapshot_timestamp: Some(1),
            user_id: None,
        },
        Request::Execute {
            query: "get from users".to_string(),
            params: Vec::new(),
            snapshot_timestamp: None,
            user_id: Some("example_id".into()),
        },
    ]
}

fn main() {
    for request in example_requests() {
        let bytes = ProtocolCodec::encode_request(&request).expect("Failed to encode request");
        let decoded_request = ProtocolCodec::decode_request(&mut BytesMut::from(&bytes[..]))
            .expect("Failed to decode request")
            .expect("Decoded request was None");
        assert_eq!(request, decoded_request);
        println!("Successfully encoded and decoded request: {:?}", request);
    }
}
