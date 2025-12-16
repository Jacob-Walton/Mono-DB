use anyhow::{Context, Result, bail};
use futures::stream::{FuturesUnordered, StreamExt};
use monodb_client::Client;
use monodb_common::Value;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

struct BenchConfig {
    name: &'static str,
    builder: Arc<dyn Fn(usize, usize) -> (String, usize) + Send + Sync>,
}

async fn get_count(client: &Client, table: &str) -> Result<usize> {
    let sql = format!("count from {table}");
    let result = client
        .query(&sql)
        .await
        .context("Failed to execute count query")?;

    println!("Count response: {:?}", result);

    let row = result.one().context("No count result returned")?;
    let count: i64 = row.get_typed("count").context("Count field missing")?;
    Ok(count as usize)
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

    let pool = Arc::new(client.pool().clone());
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
                return Ok(());
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
            response.map(|_| ())
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
        if let Err(e) = res {
            bail!("Insert failed: {e}");
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

    // Ignore result of table creation
    let _ = client.execute(table_code).await;

    let result = client.list_tables().await.context("Failed to list tables")?;

    println!("Received expected return value");
    for row in result.rows() {
        let value = row.value();
        let arr = match value {
            Value::Array(arr) => arr,
            _ => bail!("Received unexpected return value"),
        };
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
    let users_offset = get_count(&client, "users").await.unwrap_or(0);
    let testing_offset = get_count(&client, "testing").await.unwrap_or(0);

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

    Ok(())
}
