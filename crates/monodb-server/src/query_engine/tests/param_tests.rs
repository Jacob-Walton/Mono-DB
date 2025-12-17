use crate::config::StorageConfig;
use crate::query_engine::{
    ast::{Assignment, BinaryOp, Expr, Statement, TableType},
    executor::QueryExecutor,
};
use crate::storage::engine::StorageEngine;
use monodb_common::{protocol::ExecutionResult, value::Value};
use std::collections::BTreeMap;
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

// ============================================================================
// TEST SUITE: Numbered Parameters ($1, $2, etc.)
// ============================================================================

#[tokio::test]
async fn test_numbered_param_single() {
    let (mut executor, _temp_dir) = create_test_executor().await;
    create_test_collection(&mut executor, "users").await;

    // Insert test data
    insert_record(
        &mut executor,
        "users",
        vec![
            ("id", Value::Int32(1)),
            ("name", Value::String("Alice".to_string())),
            ("age", Value::Int32(30)),
        ],
    )
    .await;

    insert_record(
        &mut executor,
        "users",
        vec![
            ("id", Value::Int32(2)),
            ("name", Value::String("Bob".to_string())),
            ("age", Value::Int32(25)),
        ],
    )
    .await;

    // Set parameter: $1 = 1
    executor.set_params(vec![Value::Int32(1)]);

    // Query: get from users where id = $1
    let filter = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("id".to_string())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::NumberedParam(1)),
    };

    let get_stmt = Statement::Get {
        source: "users".to_string(),
        filter: Some(filter),
        fields: None,
        order_by: None,
        take: None,
        skip: None,
        extensions: vec![],
    };

    match executor.execute(get_stmt).await {
        Ok(ExecutionResult::Ok { data, .. }) => {
            assert_eq!(data.len(), 1, "Expected 1 row");
            if let Value::Object(obj) = &data[0] {
                assert_eq!(obj.get("name"), Some(&Value::String("Alice".to_string())));
            }
        }
        Ok(other) => panic!("Expected Ok result, got {:?}", other),
        Err(e) => panic!("Query failed: {:?}", e),
    }

    println!("✓ Numbered parameter test (single $1) passed");
}

#[tokio::test]
async fn test_numbered_param_multiple() {
    let (mut executor, _temp_dir) = create_test_executor().await;
    create_test_collection(&mut executor, "users").await;

    // Insert test data
    for i in 0..10 {
        insert_record(
            &mut executor,
            "users",
            vec![
                ("id", Value::Int32(i)),
                ("name", Value::String(format!("User{}", i))),
                ("age", Value::Int32(20 + i)),
            ],
        )
        .await;
    }

    // Set parameters: $1 = 3, $2 = 7
    executor.set_params(vec![Value::Int32(3), Value::Int32(7)]);

    // Query: get from users where id >= $1 AND id <= $2
    let filter = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("id".to_string())),
            op: BinaryOp::GtEq,
            right: Box::new(Expr::NumberedParam(1)),
        }),
        op: BinaryOp::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("id".to_string())),
            op: BinaryOp::LtEq,
            right: Box::new(Expr::NumberedParam(2)),
        }),
    };

    let get_stmt = Statement::Get {
        source: "users".to_string(),
        filter: Some(filter),
        fields: None,
        order_by: None,
        take: None,
        skip: None,
        extensions: vec![],
    };

    match executor.execute(get_stmt).await {
        Ok(ExecutionResult::Ok { data, .. }) => {
            assert_eq!(data.len(), 5, "Expected 5 rows (3,4,5,6,7)");
        }
        Ok(other) => panic!("Expected Ok result, got {:?}", other),
        Err(e) => panic!("Query failed: {:?}", e),
    }

    println!("✓ Numbered parameter test (multiple $1, $2) passed");
}

#[tokio::test]
async fn test_numbered_param_error_missing() {
    let (mut executor, _temp_dir) = create_test_executor().await;
    create_test_collection(&mut executor, "users").await;

    // Set only 1 parameter, but query uses $2
    executor.set_params(vec![Value::Int32(1)]);

    let filter = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("id".to_string())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::NumberedParam(2)), // $2 is not provided
    };

    let get_stmt = Statement::Get {
        source: "users".to_string(),
        filter: Some(filter),
        fields: None,
        order_by: None,
        take: None,
        skip: None,
        extensions: vec![],
    };

    match executor.execute(get_stmt).await {
        Err(e) => {
            let err_msg = format!("{:?}", e);
            assert!(
                err_msg.contains("Parameter $2 not provided"),
                "Expected parameter error, got: {}",
                err_msg
            );
        }
        Ok(_) => panic!("Expected error for missing parameter"),
    }

    println!("✓ Numbered parameter error test (missing param) passed");
}

// ============================================================================
// TEST SUITE: Named Parameters (:name, $name)
// ============================================================================

#[tokio::test]
async fn test_named_param_single() {
    let (mut executor, _temp_dir) = create_test_executor().await;
    create_test_collection(&mut executor, "users").await;

    // Insert test data
    insert_record(
        &mut executor,
        "users",
        vec![
            ("id", Value::Int32(1)),
            ("name", Value::String("Alice".to_string())),
            ("age", Value::Int32(30)),
        ],
    )
    .await;

    insert_record(
        &mut executor,
        "users",
        vec![
            ("id", Value::Int32(2)),
            ("name", Value::String("Bob".to_string())),
            ("age", Value::Int32(25)),
        ],
    )
    .await;

    // Set named parameters as an object
    let mut params = BTreeMap::new();
    params.insert("user_id".to_string(), Value::Int32(1));
    executor.set_params(vec![Value::Object(params)]);

    // Query: get from users where id = :user_id
    let filter = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("id".to_string())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::NamedParam("user_id".to_string())),
    };

    let get_stmt = Statement::Get {
        source: "users".to_string(),
        filter: Some(filter),
        fields: None,
        order_by: None,
        take: None,
        skip: None,
        extensions: vec![],
    };

    match executor.execute(get_stmt).await {
        Ok(ExecutionResult::Ok { data, .. }) => {
            assert_eq!(data.len(), 1, "Expected 1 row");
            if let Value::Object(obj) = &data[0] {
                assert_eq!(obj.get("name"), Some(&Value::String("Alice".to_string())));
            }
        }
        Ok(other) => panic!("Expected Ok result, got {:?}", other),
        Err(e) => panic!("Query failed: {:?}", e),
    }

    println!("✓ Named parameter test (single :user_id) passed");
}

#[tokio::test]
async fn test_named_param_multiple() {
    let (mut executor, _temp_dir) = create_test_executor().await;
    create_test_collection(&mut executor, "users").await;

    // Insert test data
    for i in 0..10 {
        insert_record(
            &mut executor,
            "users",
            vec![
                ("id", Value::Int32(i)),
                ("name", Value::String(format!("User{}", i))),
                ("age", Value::Int32(20 + i)),
            ],
        )
        .await;
    }

    // Set named parameters
    let mut params = BTreeMap::new();
    params.insert("min_age".to_string(), Value::Int32(25));
    params.insert("max_age".to_string(), Value::Int32(27));
    executor.set_params(vec![Value::Object(params)]);

    // Query: get from users where age >= :min_age AND age <= :max_age
    let filter = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("age".to_string())),
            op: BinaryOp::GtEq,
            right: Box::new(Expr::NamedParam("min_age".to_string())),
        }),
        op: BinaryOp::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("age".to_string())),
            op: BinaryOp::LtEq,
            right: Box::new(Expr::NamedParam("max_age".to_string())),
        }),
    };

    let get_stmt = Statement::Get {
        source: "users".to_string(),
        filter: Some(filter),
        fields: None,
        order_by: None,
        take: None,
        skip: None,
        extensions: vec![],
    };

    match executor.execute(get_stmt).await {
        Ok(ExecutionResult::Ok { data, .. }) => {
            assert_eq!(data.len(), 3, "Expected 3 rows (age 25, 26, 27)");
        }
        Ok(other) => panic!("Expected Ok result, got {:?}", other),
        Err(e) => panic!("Query failed: {:?}", e),
    }

    println!("✓ Named parameter test (multiple :min_age, :max_age) passed");
}

#[tokio::test]
async fn test_named_param_error_missing() {
    let (mut executor, _temp_dir) = create_test_executor().await;
    create_test_collection(&mut executor, "users").await;

    // Set named parameters, but not the one we'll use
    let mut params = BTreeMap::new();
    params.insert("other_param".to_string(), Value::Int32(1));
    executor.set_params(vec![Value::Object(params)]);

    let filter = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("id".to_string())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::NamedParam("user_id".to_string())), // Not provided
    };

    let get_stmt = Statement::Get {
        source: "users".to_string(),
        filter: Some(filter),
        fields: None,
        order_by: None,
        take: None,
        skip: None,
        extensions: vec![],
    };

    match executor.execute(get_stmt).await {
        Err(e) => {
            let err_msg = format!("{:?}", e);
            assert!(
                err_msg.contains("Named parameter 'user_id' not provided"),
                "Expected parameter error, got: {}",
                err_msg
            );
        }
        Ok(_) => panic!("Expected error for missing named parameter"),
    }

    println!("✓ Named parameter error test (missing param) passed");
}

// ============================================================================
// TEST SUITE: Parameter Type Handling
// ============================================================================

#[tokio::test]
async fn test_param_string_type() {
    let (mut executor, _temp_dir) = create_test_executor().await;
    create_test_collection(&mut executor, "users").await;

    insert_record(
        &mut executor,
        "users",
        vec![
            ("id", Value::Int32(1)),
            ("name", Value::String("Alice".to_string())),
        ],
    )
    .await;

    // Set parameter with string value
    executor.set_params(vec![Value::String("Alice".to_string())]);

    let filter = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("name".to_string())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::NumberedParam(1)),
    };

    let get_stmt = Statement::Get {
        source: "users".to_string(),
        filter: Some(filter),
        fields: None,
        order_by: None,
        take: None,
        skip: None,
        extensions: vec![],
    };

    match executor.execute(get_stmt).await {
        Ok(ExecutionResult::Ok { data, .. }) => {
            assert_eq!(data.len(), 1, "Expected 1 row");
        }
        Ok(other) => panic!("Expected Ok result, got {:?}", other),
        Err(e) => panic!("Query failed: {:?}", e),
    }

    println!("✓ Parameter string type test passed");
}

#[tokio::test]
async fn test_param_clear() {
    let (mut executor, _temp_dir) = create_test_executor().await;

    // Set some parameters
    executor.set_params(vec![Value::Int32(1), Value::Int32(2)]);

    // Clear them
    executor.clear_params();

    // Try to use a parameter - should fail
    let filter = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("id".to_string())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::NumberedParam(1)),
    };

    let get_stmt = Statement::Get {
        source: "users".to_string(),
        filter: Some(filter),
        fields: None,
        order_by: None,
        take: None,
        skip: None,
        extensions: vec![],
    };

    match executor.execute(get_stmt).await {
        Err(_) => {
            // Expected - parameter should not be available
        }
        Ok(_) => panic!("Expected error after clearing parameters"),
    }

    println!("✓ Parameter clear test passed");
}
