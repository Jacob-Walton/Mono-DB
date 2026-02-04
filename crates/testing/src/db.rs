use anyhow::Result;
use monodb_client::Client;

use crate::UserEntry;

fn dequalify_table(name: &str) -> (String, String) {
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() == 2 {
        (parts[0].to_string(), parts[1].to_string())
    } else {
        ("default".to_string(), name.to_string())
    }
}

pub async fn create_tables(conn: &mut Client) -> Result<()> {
    const TABLES: &[&str] = &["users", "measurements"];

    if let Ok(result) = conn.list_tables().await {
        let names: Vec<String> = result
            .tables
            .iter()
            .filter(|t| match dequalify_table(&t.name) {
                (ns, _) if ns == "default" => true,
                _ => false,
            })
            .map(|n| dequalify_table(&n.name).1)
            .collect();

        for name in names {
            if TABLES.contains(&name.as_str()) {
                conn.query(format!("drop table {name}")).await.unwrap();
            }
        }
    }

    let creation_result = conn
        .query(
            r#"make table users
    as relational
    fields
        id bigint
        first_name text required
        last_name text required
        email text required unique
        password_hash text required

make table measurements
    as document
    "#,
        )
        .await;

    if let Err(err) = creation_result {
        eprintln!("Error whilst creating tables: {err}");
    }

    Ok(())
}

pub async fn insert_user(conn: &mut Client, user: &UserEntry) -> Result<()> {
    const QUERY: &str = r#"put into users
    id = $1
    first_name = $2
    last_name = $3
    email = $4
    password_hash = $5"#;

    conn.query_with_params(
        QUERY,
        vec![
            user.id.clone(),
            user.first_name.clone(),
            user.last_name.clone(),
            user.email.clone(),
            user.password_hash.clone(),
        ],
    )
    .await
    .unwrap();

    Ok(())
}
