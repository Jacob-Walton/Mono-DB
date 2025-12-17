use std::time::Instant;

use monodb_client::{Client, Row};
use monodb_common::Value;

#[derive(Debug)]
struct User {
    id: i32,
    first_name: String,
    last_name: String,
    email: String,
}

impl User {
    fn from_row(row: Row) -> Self {
        User {
            id: row.get_typed("id").expect("id missing or not an i32"),
            first_name: row
                .get_typed("first_name")
                .expect("first_name missing or not a string"),
            last_name: row
                .get_typed("last_name")
                .expect("last_name missing or not a string"),
            email: row
                .get_typed("email")
                .expect("email missing or not a string"),
        }
    }
}

#[tokio::main]
async fn main() {
    let client = Client::connect("localhost:7899").await.unwrap();

    let start_time = Instant::now();

    let results = client
        .query("get from users order by id desc take 100")
        .await
        .unwrap();

    let elapsed = start_time.elapsed();

    println!("Query returned {} rows ({elapsed:?})", results.count_rows());

    let mut users: Vec<User> = Vec::with_capacity(10);

    for user in results.rows() {
        users.push(User::from_row(user));
    }

    println!("Fetched {} users", users.len());

    // Test query with parameters
    let start_time = Instant::now();

    let param_results = client
        .query_with_params(
            "get from users where id > $1 order by id desc",
            vec![Value::Int32(50)],
        )
        .await
        .unwrap();

    let elapsed = start_time.elapsed();
    println!(
        "Parameterized query returned {} rows ({elapsed:?})",
        param_results.count_rows()
    );

    client.close().await.unwrap();
}
