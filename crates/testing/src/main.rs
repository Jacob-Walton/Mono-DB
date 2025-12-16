use anyhow::{Context, Result, bail};
use futures::stream::{FuturesUnordered, StreamExt};
use monodb_client::Client;
use monodb_common::{
    Value,
    protocol::{ExecutionResult, Response},
};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

struct BenchConfig {
    name: &'static str,
    builder: Arc<dyn Fn(usize, usize) -> (String, usize) + Send + Sync>,
}

async fn get_count(client: &mut Client, table: &str) -> Result<usize> {
    let pool = client.pool().await.clone();
    let mut conn = pool
        .get()
        .await
        .context("Failed to get connection from pool")?;
    let sql = format!("count from {table}");
    let response = conn
        .execute(sql)
        .await
        .context("Failed to execute count query")?;

    println!("Count response: {:?}", response);

    match response {
        Response::Success { result } => {
            let first = result.get(0).cloned().context("No result returned")?;
            match first {
                ExecutionResult::Ok { data, .. } => {
                    let first_row = data.get(0).cloned().context("No data returned")?;
                    match first_row {
                        Value::Object(map) => {
                            let count_value = map.get("count").context("Count field missing")?;
                            let count =
                                count_value.as_i64().context("Count field not an integer")?;
                            Ok(count as usize)
                        }
                        _ => bail!("Unexpected data format for count result"),
                    }
                }
                _ => bail!("Unexpected ExecutionResult type for count query"),
            }
        }
        _ => bail!("Unexpected Response type for count query"),
    }
}

/// Run a write benchmark against the configured target.
async fn run_benchmark(
    cfg: BenchConfig,
    client: Client,
    count: usize,
    concurrency: usize,
    batch_size: usize,
) -> Result<()> {
    println!("\n--- {} ---", cfg.name);

    let start_time = std::time::Instant::now();

    let pool = Arc::new(client.pool().await.clone());
    let latencies = Arc::new(AsyncMutex::new(Vec::with_capacity(count)));

    let insert_futures = (0..count).step_by(batch_size).map(|base| {
        let pool = Arc::clone(&pool);
        let lat_clone = Arc::clone(&latencies);
        let builder = Arc::clone(&cfg.builder);
        async move {
            let mut conn = pool
                .get()
                .await
                .expect("Failed to get connection from pool");

            let (batch_sql, actual) = builder(base, batch_size);
            if actual == 0 {
                pool.return_connection(conn);
                return Ok(Response::Success { result: vec![] });
            }

            let start = std::time::Instant::now();
            let response = conn.execute(batch_sql).await;
            let elapsed = start.elapsed().as_nanos();

            let per_insert = elapsed / (actual as u128);
            let mut guard = lat_clone.lock().await;
            for _ in 0..actual {
                guard.push(per_insert);
            }

            pool.return_connection(conn);
            response
        }
    });

    let mut stream = FuturesUnordered::new();
    let mut in_flight = 0;
    let mut insert_iter = insert_futures.into_iter();

    while in_flight < concurrency {
        if let Some(fut) = insert_iter.next() {
            stream.push(fut);
            in_flight += 1;
        } else {
            break;
        }
    }

    while let Some(res) = stream.next().await {
        in_flight -= 1;
        match res {
            Ok(Response::Success { .. }) => {}
            Ok(other) => bail!("Unexpected response type on insert: {other:?}"),
            Err(e) => bail!("Insert failed: {e}"),
        }
        if let Some(fut) = insert_iter.next() {
            stream.push(fut);
            in_flight += 1;
        }
    }

    let duration = start_time.elapsed();
    println!("Inserted {count} items in {:?}", duration);

    let mut lat_vec = latencies.lock().await;
    if !lat_vec.is_empty() {
        lat_vec.sort_unstable();
        let count = lat_vec.len();
        let sum: u128 = lat_vec.iter().copied().sum();
        let mean_ns = sum as f64 / count as f64;
        let idx = |p: f64| -> usize {
            let mut i = (count as f64 * p).floor() as usize;
            if i >= count {
                i = count - 1;
            }
            i
        };
        let p50 = lat_vec[idx(0.50)];
        let p90 = lat_vec[idx(0.90)];
        let p99 = lat_vec[idx(0.99)];
        let max = *lat_vec.last().unwrap();

        println!(
            "Per-request latency (ns): count={} mean={:.0} p50={} p90={} p99={} max={}",
            count, mean_ns, p50, p90, p99, max
        );
    } else {
        println!("No latency samples recorded");
    }

    // Calculate ops/s
    let ops_per_sec = count as f64 / duration.as_secs_f64();
    match ops_per_sec {
        ops if ops < 1_000.0 => println!("Throughput: {:.2} ops/s", ops),
        ops if ops < 1_000_000.0 => println!("Throughput: {:.2} Kops/s", ops / 1_000.0),
        ops => println!("Throughput: {:.2} Mops/s", ops / 1_000_000.0),
    }

    Ok(())
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let client = Client::connect("localhost:7899")
        .await
        .context("Failed to connect to server")?;

    let table_code = r#"
make table users
    as relational
    fields
        id int primary key
        first_name text
        last_name text
        email text

make table testing
    as document

make table sessions
    as keyspace
    persistence "memory"
"#;

    let pool = client.pool().await.clone();
    let mut conn = pool
        .get()
        .await
        .context("Failed to get connection from pool")?;

    // Ignore result of table creation
    let _ = conn.execute(table_code.to_string()).await;

    let response = conn.list_tables().await.context("Failed to list tables")?;

    let result = match response {
        Response::Success { result } => result.get(0).cloned().context("No result returned")?,
        _ => bail!("Received unexpected response type"),
    };

    match result {
        ExecutionResult::Ok { data, .. } => {
            for value in data {
                let arr = match value {
                    Value::Array(arr) => arr,
                    _ => bail!("Received unexpected return value"),
                };
                println!("Received expected return value");
                for item in arr {
                    let arr = match item {
                        Value::Array(arr) => arr,
                        _ => bail!("Found unexpected value in table listing"),
                    };
                    if arr.len() != 2 {
                        bail!("Received invalid table array");
                    }
                    let name = arr
                        .get(1)
                        .and_then(|v| v.as_string())
                        .context("Table name missing or not a string")?;
                    let r#type = arr
                        .get(0)
                        .and_then(|v| v.as_string())
                        .context("Table type missing or not a string")?;
                    println!("{} (type: {})", name, r#type);
                }
            }
        }
        _ => bail!("Received unexpected ExecutionResult"),
    }

    // Test inserting a large number of records using multiple connections
    const COUNT: usize = 1_000_000;
    const CONCURRENCY: usize = 16;
    // Batch size: number of logical inserts per Execute request
    const BATCH_SIZE: usize = 256;

    let keyspace = BenchConfig {
        name: "In-memory keyspace (sessions)",
        builder: Arc::new(|base, batch_size| {
            let mut batch_sql = String::new();
            let mut actual = 0usize;
            for j in 0..batch_size {
                let i = base + j;
                if i >= COUNT {
                    break;
                }
                batch_sql.push_str(&format!(
                    "put into sessions\n    key = \"session:{i}\"\n    value = \"payload{i}\"\n\n"
                ));
                actual += 1;
            }
            (batch_sql, actual)
        }),
    };

    // Get existing counts to offset IDs
    let mut client_mut = client.clone();
    let users_offset = get_count(&mut client_mut, "users").await.unwrap_or(0);
    let testing_offset = get_count(&mut client_mut, "testing").await.unwrap_or(0);

    // Print offsets
    println!(
        "Existing users count: {}, offsetting IDs by this amount",
        users_offset
    );
    println!(
        "Existing testing count: {}, offsetting IDs by this amount",
        testing_offset
    );

    let relational = BenchConfig {
        name: "Relational table (users)",
        builder: Arc::new(move |base, batch_size| {
            let mut batch_sql = String::new();
            let mut actual = 0usize;
            for j in 0..batch_size {
                let i = base + j + users_offset;
                if base + j >= COUNT {
                    break;
                }
                let first_name = format!("First{i}");
                let last_name = format!("Last{i}");
                let email = format!("{first_name}.{last_name}@example.com");
                batch_sql.push_str(&format!(
                    "put into users\n    id = {i}\n    first_name = \"{first_name}\"\n    last_name = \"{last_name}\"\n    email = \"{email}\"\n\n"
                ));
                actual += 1;
            }
            (batch_sql, actual)
        }),
    };

    let collection = BenchConfig {
        name: "Document collection (testing)",
        builder: Arc::new(move |base, batch_size| {
            let mut batch_sql = String::new();
            let mut actual = 0usize;
            for j in 0..batch_size {
                let i = base + j + testing_offset;
                if base + j >= COUNT {
                    break;
                }
                batch_sql.push_str(&format!(
                    "put into testing\n    _id = {i}\n    first_name = \"DocFirst{i}\"\n    last_name = \"DocLast{i}\"\n    email = \"doc{i}@example.com\"\n\n"
                ));
                actual += 1;
            }
            (batch_sql, actual)
        }),
    };

    run_benchmark(relational, client.clone(), COUNT, CONCURRENCY, BATCH_SIZE).await?;
    run_benchmark(collection, client.clone(), COUNT, CONCURRENCY, BATCH_SIZE).await?;
    // Keyspace is in-memory, always run
    run_benchmark(keyspace, client.clone(), COUNT, CONCURRENCY, BATCH_SIZE).await?;

    pool.return_connection(conn);

    Ok(())
}
