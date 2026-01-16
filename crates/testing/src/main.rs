use anyhow::Result;
use monodb_client::Client;
use monodb_common::Value;

const QUERY: &'static str = r#"put into users
    first_name = $1
    last_name = $2
    email = $3"#;

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::connect("127.0.0.1:6432").await?;
    let tables = client.list_tables().await?;
    println!("Tables: {:?}", tables);

    // Make users table if it doesn't exist
    if !tables.tables.iter().any(|t| t.name == "users") {
        client
            .query(
                r#"make table users
    as relational
    fields
        id bigint primary key
        first_name text required
        last_name text required
        email text required unique"#,
            )
            .await?;
    }

    let users = client.query("get from users").await?;
    if let Some(user) = users.rows().first() {
        println!("User: {:?}", user);
    }

    for i in 0..100 {
        client
            .query_with_params(
                QUERY,
                vec![
                    Value::String(format!("First{}", i)),
                    Value::String(format!("Last{}", i)),
                    Value::String(format!("first.last{}@example.com", i)),
                ],
            )
            .await?;
    }

    Ok(())
}
