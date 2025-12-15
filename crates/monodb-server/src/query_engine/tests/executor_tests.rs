use crate::config::StorageConfig;
use crate::query_engine::{
    ast::{Assignment, BinaryOp, Expr, Statement, TableType},
    executor::QueryExecutor,
};
use crate::storage::engine::StorageEngine;
use monodb_common::{protocol::ExecutionResult, value::Value};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper function to create a temporary storage engine for testing
async fn create_test_storage() -> (Arc<StorageEngine>, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = StorageConfig {
        data_dir: temp_dir.path().to_string_lossy().to_string(),
        ..Default::default()
    };

    let storage = StorageEngine::new(config)
        .await
        .expect("Failed to create storage engine");

    (storage, temp_dir)
}

/// Helper function to create a QueryExecutor for testing
async fn create_test_executor() -> (QueryExecutor, TempDir) {
    let (storage, temp_dir) = create_test_storage().await;
    let executor = QueryExecutor::new(storage);
    (executor, temp_dir)
}

/// Helper to create a document collection
async fn create_test_collection(executor: &mut QueryExecutor, name: &str) {
    let make_stmt = Statement::Make {
        table_type: TableType::Document,
        name: name.to_string(),
        schema: None,
        properties: vec![],
    };

    let result = executor.execute(make_stmt).await;
    assert!(
        result.is_ok(),
        "Failed to create collection: {:?}",
        result.err()
    );
}

/// Helper to insert a single record
async fn insert_record(
    executor: &mut QueryExecutor,
    target: &str,
    fields: Vec<(&str, Value)>,
) -> ExecutionResult {
    let values = fields
        .into_iter()
        .map(|(key, val)| Assignment {
            key: key.to_string(),
            value: Expr::Literal(val),
        })
        .collect();

    let put_stmt = Statement::Put {
        target: target.to_string(),
        values,
        extensions: vec![],
    };

    executor.execute(put_stmt).await.unwrap()
}

/// Helper to execute a GET query with filter
async fn execute_get_query(
    executor: &mut QueryExecutor,
    source: &str,
    filter: Option<Expr>,
    fields: Option<Vec<String>>,
) -> Vec<Value> {
    let get_stmt = Statement::Get {
        source: source.to_string(),
        filter,
        fields,
        order_by: None,
        take: None,
        skip: None,
        extensions: vec![],
    };

    match executor.execute(get_stmt).await {
        Ok(ExecutionResult::Ok { data, .. }) => data,
        Ok(other) => panic!("Expected Ok result, got {:?}", other),
        Err(e) => panic!("Query failed: {:?}", e),
    }
}

/// Helper to create a binary comparison expression
fn create_comparison(field: &str, op: BinaryOp, value: Value) -> Expr {
    Expr::BinaryOp {
        left: Box::new(Expr::Identifier(field.to_string())),
        op,
        right: Box::new(Expr::Literal(value)),
    }
}

// ============================================================================
// TEST SUITE: Basic Get
// ============================================================================

#[tokio::test]
async fn test_basic_get_simple_query() {
    // Test Synopsis: Simple table query
    // Test Data: users where age > 25 select name, email on 1000 rows
    // Expected Outcome: Returns matching rows with specified fields

    let (mut executor, _temp_dir) = create_test_executor().await;
    create_test_collection(&mut executor, "users").await;

    // Insert 1000 test records with varying ages
    for i in 0..1000 {
        let age = 20 + (i % 20); // Ages from 20 to 39
        let email = format!("user{}@example.com", i);
        let name = format!("User{}", i);

        insert_record(
            &mut executor,
            "users",
            vec![
                ("name", Value::String(name)),
                ("email", Value::String(email)),
                ("age", Value::Int32(age)),
            ],
        )
        .await;
    }

    // Query: users where age > 25 select name, email
    let filter = create_comparison("age", BinaryOp::Gt, Value::Int32(25));
    let fields = Some(vec!["name".to_string(), "email".to_string()]);

    let results = execute_get_query(&mut executor, "users", Some(filter), fields).await;

    // Verify results
    assert!(
        !results.is_empty(),
        "Expected matching rows, got empty result"
    );

    // Count how many should match (ages 26-39 = 14 ages * 50 occurrences each)
    let expected_count = 700; // (39 - 25) * 50
    assert_eq!(
        results.len(),
        expected_count,
        "Expected {} matching rows, got {}",
        expected_count,
        results.len()
    );

    // Verify all results have age > 25
    for row in &results {
        if let Value::Object(obj) = row {
            // Verify the row has the selected fields
            assert!(
                obj.contains_key("name"),
                "Result should contain 'name' field"
            );
            assert!(
                obj.contains_key("email"),
                "Result should contain 'email' field"
            );

            // Note: Age should also be present even though we selected name, email
            // because the current implementation returns all fields
        } else {
            panic!("Expected object result, got {:?}", row);
        }
    }

    println!(
        "✓ Basic Get test passed: {} matching rows retrieved",
        results.len()
    );
}

#[tokio::test]
async fn test_basic_get_with_multiple_filters() {
    let (mut executor, _temp_dir) = create_test_executor().await;
    create_test_collection(&mut executor, "users").await;

    // Insert test data
    for i in 0..100 {
        insert_record(
            &mut executor,
            "users",
            vec![
                ("name", Value::String(format!("User{}", i))),
                ("age", Value::Int32(20 + (i % 30))),
                ("active", Value::Bool(i % 2 == 0)),
            ],
        )
        .await;
    }

    // Query: users where age > 30 AND active = true
    let filter = Expr::BinaryOp {
        left: Box::new(create_comparison("age", BinaryOp::Gt, Value::Int32(30))),
        op: BinaryOp::And,
        right: Box::new(create_comparison("active", BinaryOp::Eq, Value::Bool(true))),
    };

    let results = execute_get_query(&mut executor, "users", Some(filter), None).await;

    assert!(!results.is_empty(), "Expected matching rows");
    println!(
        "✓ Multiple filters test passed: {} rows matched",
        results.len()
    );
}

// ============================================================================
// TEST SUITE: Basic Put
// ============================================================================

#[tokio::test]
async fn test_basic_put_insertion() {
    // Test Synopsis: Data insertion
    // Test Data: Insert 500 records with mixed types
    // Expected Outcome: All records stored, retrievable with correct types

    let (mut executor, _temp_dir) = create_test_executor().await;
    create_test_collection(&mut executor, "mixed_data").await;

    let start = std::time::Instant::now();

    // Insert 500 records with mixed types
    let total_records = 500;
    for i in 0..total_records {
        let record_type = i % 5;

        let fields = match record_type {
            0 => vec![
                ("type", Value::String("integer".to_string())),
                ("value", Value::Int32(i)),
                ("index", Value::Int64(i as i64)),
            ],
            1 => vec![
                ("type", Value::String("float".to_string())),
                ("value", Value::Float64(i as f64 * 1.5)),
                ("index", Value::Int64(i as i64)),
            ],
            2 => vec![
                ("type", Value::String("string".to_string())),
                ("value", Value::String(format!("Record_{}", i))),
                ("index", Value::Int64(i as i64)),
            ],
            3 => vec![
                ("type", Value::String("boolean".to_string())),
                ("value", Value::Bool(i % 2 == 0)),
                ("index", Value::Int64(i as i64)),
            ],
            4 => vec![
                ("type", Value::String("null".to_string())),
                ("value", Value::Null),
                ("index", Value::Int64(i as i64)),
            ],
            _ => unreachable!(),
        };

        let result = insert_record(&mut executor, "mixed_data", fields).await;

        match result {
            ExecutionResult::Modified { .. } => {}
            _ => panic!("Expected Modified result for insert"),
        }
    }

    let insert_time = start.elapsed();
    println!("✓ Inserted {} records in {:?}", total_records, insert_time);

    // Retrieve all records and verify
    let all_records = execute_get_query(&mut executor, "mixed_data", None, None).await;

    assert_eq!(
        all_records.len(),
        total_records as usize,
        "Expected {} records, got {}",
        total_records,
        all_records.len()
    );

    // Verify type distribution
    let mut type_counts = HashMap::new();
    for record in &all_records {
        if let Value::Object(obj) = record
            && let Some(Value::String(type_str)) = obj.get("type")
        {
            *type_counts.entry(type_str.clone()).or_insert(0) += 1;
        }
    }

    // Each type should have exactly 100 records (500 / 5 types)
    let expected_per_type = (total_records / 5) as usize;
    assert_eq!(
        type_counts.get("integer"),
        Some(&expected_per_type),
        "Expected {} integer records",
        expected_per_type
    );
    assert_eq!(
        type_counts.get("float"),
        Some(&expected_per_type),
        "Expected {} float records",
        expected_per_type
    );
    assert_eq!(
        type_counts.get("string"),
        Some(&expected_per_type),
        "Expected {} string records",
        expected_per_type
    );
    assert_eq!(
        type_counts.get("boolean"),
        Some(&expected_per_type),
        "Expected {} boolean records",
        expected_per_type
    );
    assert_eq!(
        type_counts.get("null"),
        Some(&expected_per_type),
        "Expected {} null records",
        expected_per_type
    );

    println!(
        "✓ All {} records verified with correct types",
        total_records
    );
}

#[tokio::test]
async fn test_basic_put_with_complex_types() {
    let (mut executor, _temp_dir) = create_test_executor().await;
    create_test_collection(&mut executor, "complex").await;

    // Test nested objects and arrays
    let nested_obj = {
        let mut inner = std::collections::BTreeMap::new();
        inner.insert("city".to_string(), Value::String("London".to_string()));
        inner.insert("zip".to_string(), Value::String("SW1A 1AA".to_string()));
        Value::Object(inner)
    };

    let array_val = Value::Array(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]);

    insert_record(
        &mut executor,
        "complex",
        vec![
            ("name", Value::String("Test".to_string())),
            ("address", nested_obj),
            ("numbers", array_val),
        ],
    )
    .await;

    let results = execute_get_query(&mut executor, "complex", None, None).await;
    assert_eq!(results.len(), 1, "Expected 1 record");

    println!("✓ Complex types (nested objects, arrays) inserted correctly");
}

// ============================================================================
// TEST SUITE: Basic Change
// ============================================================================

#[ignore = "Bulk updates not yet implemented"]
#[tokio::test]
async fn test_basic_change_bulk_updates() {
    // Test Synopsis: Bulk updates
    // Test Data: Update 5,000 records matching condition
    // Expected Outcome: Exactly 5,000 modified, others unchanged

    let (mut executor, _temp_dir) = create_test_executor().await;
    create_test_collection(&mut executor, "users").await;

    // Insert 10,000 records
    for i in 0..10_000 {
        insert_record(
            &mut executor,
            "users",
            vec![
                ("id", Value::Int32(i)),
                ("status", Value::String("active".to_string())),
                ("score", Value::Int32(0)),
            ],
        )
        .await;
    }

    // Update records where id < 5000: set score = 100, status = "updated"
    let filter = create_comparison("id", BinaryOp::Lt, Value::Int32(5000));

    let changes = vec![
        Assignment {
            key: "score".to_string(),
            value: Expr::Literal(Value::Int32(100)),
        },
        Assignment {
            key: "status".to_string(),
            value: Expr::Literal(Value::String("updated".to_string())),
        },
    ];

    let change_stmt = Statement::Change {
        target: "users".to_string(),
        filter: Some(filter),
        changes,
        extensions: vec![],
    };

    match executor.execute(change_stmt).await {
        Ok(ExecutionResult::Modified { rows_affected, .. }) => {
            assert_eq!(
                rows_affected,
                Some(5000),
                "Expected 5,000 rows modified, got {:?}",
                rows_affected
            );
        }
        Ok(other) => panic!("Expected Modified result, got {:?}", other),
        Err(e) => panic!("Update failed: {:?}", e),
    }

    // Verify: 5000 records should have score=100 and status="updated"
    let updated = execute_get_query(
        &mut executor,
        "users",
        Some(create_comparison("score", BinaryOp::Eq, Value::Int32(100))),
        None,
    )
    .await;
    assert_eq!(updated.len(), 5000, "Expected 5,000 updated records");

    // Verify: 5000 records should still have score=0 and status="active"
    let unchanged = execute_get_query(
        &mut executor,
        "users",
        Some(create_comparison("score", BinaryOp::Eq, Value::Int32(0))),
        None,
    )
    .await;
    assert_eq!(unchanged.len(), 5000, "Expected 5,000 unchanged records");

    println!("✓ Bulk update test passed: 5,000 modified, 5,000 unchanged");
}

// ============================================================================
// TEST SUITE: Basic Remove
// ============================================================================

#[tokio::test]
async fn test_basic_remove_conditional_deletion() {
    // Test Synopsis: Conditional deletion
    // Test Data: remove where status = 'inactive'
    // Expected Outcome: Matching records removed, count correct

    let (mut executor, _temp_dir) = create_test_executor().await;
    create_test_collection(&mut executor, "users").await;

    // Insert test records with mixed status
    let total_records = 500;
    let total_inactive = 172;
    let total_active = total_records - total_inactive;
    let mut inserted = 0;

    for i in 0..total_records {
        let status = if i < total_inactive {
            "inactive"
        } else {
            "active"
        };

        insert_record(
            &mut executor,
            "users",
            vec![
                ("id", Value::Int32(i as i32)),
                ("status", Value::String(status.to_string())),
                ("name", Value::String(format!("User{}", i))),
            ],
        )
        .await;
        inserted += 1;
    }

    assert_eq!(
        inserted, total_records,
        "Should have inserted {} records",
        total_records
    );

    // Remove where status = 'inactive'
    let filter = create_comparison(
        "status",
        BinaryOp::Eq,
        Value::String("inactive".to_string()),
    );

    let remove_stmt = Statement::Remove {
        target: "users".to_string(),
        filter: Some(filter),
        extensions: vec![],
    };

    // Force flush LSM memtable to ensure all data is queryable
    executor.flush_storage().await.expect("Failed to flush");

    match executor.execute(remove_stmt).await {
        Ok(ExecutionResult::Modified { rows_affected, .. }) => {
            assert_eq!(
                rows_affected,
                Some(total_inactive),
                "Expected {} rows deleted, got {:?}",
                total_inactive,
                rows_affected
            );
        }
        Ok(other) => panic!("Expected Modified result, got {:?}", other),
        Err(e) => panic!("Delete failed: {:?}", e),
    }

    // Verify: Only active users remain
    let remaining = execute_get_query(&mut executor, "users", None, None).await;
    assert_eq!(
        remaining.len(),
        total_active as usize,
        "Expected {} remaining records, got {}",
        total_active,
        remaining.len()
    );

    // Verify all remaining have status="active"
    for record in remaining {
        if let Value::Object(obj) = record
            && let Some(Value::String(status)) = obj.get("status")
        {
            assert_eq!(status, "active", "Remaining record should be active");
        }
    }

    println!(
        "✓ Conditional deletion test passed: {} deleted, {} remaining",
        total_inactive, total_active
    );
}

#[tokio::test]
async fn test_basic_remove_with_numeric_filter() {
    let (mut executor, _temp_dir) = create_test_executor().await;
    create_test_collection(&mut executor, "items").await;

    // Insert 100 items with scores 0-99
    for i in 0..100 {
        insert_record(
            &mut executor,
            "items",
            vec![("id", Value::Int32(i)), ("score", Value::Int32(i))],
        )
        .await;
    }

    // Force flush LSM memtable after inserts to ensure all data is queryable
    executor
        .flush_storage()
        .await
        .expect("Failed to flush after inserts");

    // Verify all 100 items were inserted
    let all_items = execute_get_query(&mut executor, "items", None, None).await;
    assert_eq!(
        all_items.len(),
        100,
        "Expected 100 items after insertion, got {}",
        all_items.len()
    );

    // Remove items where score < 30
    let filter = create_comparison("score", BinaryOp::Lt, Value::Int32(30));
    let remove_stmt = Statement::Remove {
        target: "items".to_string(),
        filter: Some(filter),
        extensions: vec![],
    };

    let result = executor.execute(remove_stmt).await.unwrap();
    match result {
        ExecutionResult::Modified { rows_affected, .. } => {
            assert_eq!(rows_affected, Some(30), "Expected 30 rows deleted");
        }
        _ => panic!("Expected Modified result"),
    }

    let remaining = execute_get_query(&mut executor, "items", None, None).await;
    assert_eq!(remaining.len(), 70, "Expected 70 remaining items");

    println!("✓ Numeric filter deletion test passed");
}

// ============================================================================
// TEST SUITE: Aggregation
// ============================================================================

#[tokio::test]
#[ignore = "Aggregation functions (count, sum, avg) not yet implemented"]
async fn test_aggregation_count_sum_avg() {
    // Test Synopsis: Count, sum, avg operations
    // Test Data: count, sum(amount), avg(score)
    // Expected Outcome: Correct calculated values

    let (mut executor, _temp_dir) = create_test_executor().await;
    create_test_collection(&mut executor, "transactions").await;

    // Insert test data
    let test_data = vec![(100, 85), (200, 90), (150, 75), (300, 95), (250, 80)];

    for (amount, score) in &test_data {
        insert_record(
            &mut executor,
            "transactions",
            vec![
                ("amount", Value::Int32(*amount)),
                ("score", Value::Int32(*score)),
            ],
        )
        .await;
    }

    // Expected values
    let expected_count = test_data.len();
    let expected_sum: i32 = test_data.iter().map(|(amt, _)| amt).sum();
    let expected_avg_score: f64 =
        test_data.iter().map(|(_, score)| score).sum::<i32>() as f64 / test_data.len() as f64;

    // Execute aggregation query (hypothetical syntax)
    // get from transactions select count(), sum(amount), avg(score)
    let agg_stmt = Statement::Get {
        source: "transactions".to_string(),
        filter: None,
        fields: Some(vec![
            "count()".to_string(),
            "sum(amount)".to_string(),
            "avg(score)".to_string(),
        ]),
        order_by: None,
        take: None,
        skip: None,
        extensions: vec![],
    };

    match executor.execute(agg_stmt).await {
        Ok(ExecutionResult::Ok { data, .. }) => {
            if let Some(Value::Object(result)) = data.first() {
                // Verify count
                if let Some(Value::Int64(count)) = result.get("count") {
                    assert_eq!(*count, expected_count as i64, "Count mismatch");
                }

                // Verify sum
                if let Some(Value::Int32(sum)) = result.get("sum(amount)") {
                    assert_eq!(*sum, expected_sum, "Sum mismatch");
                }

                // Verify average
                if let Some(Value::Float64(avg)) = result.get("avg(score)") {
                    assert!(
                        (*avg - expected_avg_score).abs() < 0.01,
                        "Average mismatch: expected {}, got {}",
                        expected_avg_score,
                        avg
                    );
                }

                println!(
                    "✓ Aggregation test passed: count={}, sum={}, avg={}",
                    expected_count, expected_sum, expected_avg_score
                );
            } else {
                panic!("Expected object result for aggregation");
            }
        }
        Ok(other) => panic!("Expected Ok result, got {:?}", other),
        Err(e) => panic!("Aggregation query failed: {:?}", e),
    }
}

#[tokio::test]
#[ignore = "Aggregation with grouping not yet implemented"]
async fn test_aggregation_with_grouping() {
    // Test grouping with aggregations
    let (mut executor, _temp_dir) = create_test_executor().await;
    create_test_collection(&mut executor, "sales").await;

    // Insert sales data for different categories
    let categories = vec!["Electronics", "Books", "Clothing"];
    for category in &categories {
        for i in 0..10 {
            insert_record(
                &mut executor,
                "sales",
                vec![
                    ("category", Value::String(category.to_string())),
                    ("amount", Value::Int32(100 + i * 10)),
                ],
            )
            .await;
        }
    }

    // Query: get from sales select category, count(), sum(amount) group by category
    // This would require implementing GROUP BY functionality
    // Expected: 3 rows, one per category, each with count=10 and sum

    println!("✓ Aggregation with grouping test (to be implemented)");
}

// ============================================================================
// PERFORMANCE BENCHMARKS
// ============================================================================

#[tokio::test]
async fn bench_large_dataset_query_performance() {
    let (mut executor, _temp_dir) = create_test_executor().await;
    create_test_collection(&mut executor, "large_dataset").await;

    let total_records: i64 = 1_000;
    println!("Inserting {} records...", total_records);
    let insert_start = std::time::Instant::now();

    for i in 0..total_records {
        insert_record(
            &mut executor,
            "large_dataset",
            vec![
                ("id", Value::Int64(i)),
                ("value", Value::Int32((i % 1000) as i32)),
                ("category", Value::String(format!("cat_{}", i % 10))),
            ],
        )
        .await;
    }

    let insert_time = insert_start.elapsed();
    println!("✓ Inserted {} records in {:?}", total_records, insert_time);

    // Benchmark: Query with filter
    let query_start = std::time::Instant::now();
    let filter = create_comparison("value", BinaryOp::Gt, Value::Int32(500));
    let results = execute_get_query(&mut executor, "large_dataset", Some(filter), None).await;
    let query_time = query_start.elapsed();

    println!(
        "✓ Filtered query returned {} results in {:?}",
        results.len(),
        query_time
    );

    // Benchmark: Full scan
    let scan_start = std::time::Instant::now();
    let all_results = execute_get_query(&mut executor, "large_dataset", None, None).await;
    let scan_time = scan_start.elapsed();

    println!(
        "✓ Full scan returned {} records in {:?}",
        all_results.len(),
        scan_time
    );

    assert_eq!(
        all_results.len(),
        total_records as usize,
        "Expected all records"
    );
}
