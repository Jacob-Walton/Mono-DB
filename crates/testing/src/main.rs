use anyhow::{Result, bail};
use monodb_client::Client;
use monodb_common::protocol::{ExecutionResult, Response};

#[tokio::main]
pub async fn main() -> Result<()> {
    let client = Client::connect("localhost:7899").await;

    let table_code = r#"
make table users
    as relational
    fields
        id int primary key unique
        first_name text
        last_name text
        email text unique

make table testing
    as document

make table sessions
    as keyspace
    persistence "memory"
"#;

    match client {
        Ok(client) => {
            let pool = client.pool().await;
            let connection = pool.get().await;

            match connection {
                Ok(mut conn) => {
                    let _ = conn.execute(table_code.to_string()).await;

                    let response = conn.list_tables().await;

                    match response {
                        Ok(resp) => match resp {
                            Response::Success { result } => {
                                let result = result.get(0).unwrap();

                                match result {
                                    ExecutionResult::Ok { data, .. } => {
                                        println!("Data: {data}");
                                    }
                                    _ => bail!("Received unexpected ExecutionResult"),
                                }
                            }
                            _ => bail!("Received unexpected response type"),
                        },
                        Err(e) => {
                            bail!(e)
                        }
                    }
                }
                Err(e) => {
                    bail!(e)
                }
            }
        }
        Err(e) => {
            bail!(e)
        }
    }
    Ok(())
}
