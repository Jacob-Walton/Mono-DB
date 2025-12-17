# Implementing Query Parameters in MonoDB

## Introduction

When I first designed the query language for MonoDB, I focused on getting the basic operations working - retrieving data, inserting records, and filtering results. However, I quickly realised that hardcoding values directly into queries created a significant limitation. Consider a simple query like:

```
get from users where id = 42
```

This works fine for a one-off query, but what happens when a client application needs to run this query hundreds of times with different ID values? The application would need to construct a new query string each time, concatenating the ID value into the query text. This approach has several problems that I needed to address.

First, there's the issue of SQL injection. If user input is directly concatenated into query strings, malicious users could potentially inject harmful commands. While MonoDB's query language isn't SQL, the same principle applies - we shouldn't trust user-provided values to be safely embedded in query text.

Second, there's a performance consideration. Every time a query string changes, the server must re-parse it from scratch. If we could separate the query structure from its parameter values, we could potentially cache the parsed query and reuse it with different values.

Third, the code becomes cleaner and more maintainable when queries and their parameters are separate concerns. The query defines what operation to perform, while the parameters provide the specific values for that operation.

These considerations led me to implement query parameter support, allowing queries like:

```
get from users where id = $1
```

where `$1` represents a placeholder that gets filled in with an actual value at execution time.

## Designing the Parameter Syntax

Before writing any code, I needed to decide what the parameter syntax should look like. I researched how other databases handle this and found two common approaches.

PostgreSQL uses numbered parameters with a dollar sign prefix: `$1`, `$2`, `$3`, and so on. This approach is straightforward - the numbers correspond to positions in a parameter array. The first parameter you provide fills in `$1`, the second fills in `$2`, and so forth.

Oracle and some other databases use named parameters with a colon prefix: `:name`, `:email`, `:age`. This approach is more verbose but can make complex queries more readable, especially when the same parameter appears multiple times or when there are many parameters.

I decided to support both syntaxes. The numbered approach would be simpler for basic use cases, while named parameters would provide better readability for complex queries. Supporting both also meant that developers familiar with either PostgreSQL or Oracle conventions would find something familiar.

## Understanding the Query Pipeline

To implement parameters, I first needed to understand how a query flows through the system. When a client sends a query to MonoDB, it passes through several stages:

The **lexer** reads the raw query text character by character and breaks it into tokens. For example, the query `get from users` becomes four tokens: the keyword `get`, the keyword `from`, the identifier `users`. The lexer doesn't understand the meaning of these tokens - it simply categorises each piece of text.

The **parser** takes the stream of tokens from the lexer and builds an Abstract Syntax Tree (AST). The AST represents the structure of the query in a form that's easier for the computer to work with. A `get` statement becomes a tree node containing the source table, any filter conditions, field selections, and so on.

The **executor** walks through the AST and performs the actual operations. For a `get` statement, it reads from storage, applies filters, and returns the matching rows.

To support parameters, I needed to modify each of these stages. The lexer needed to recognise parameter tokens, the parser needed to represent them in the AST, and the executor needed to substitute actual values for the parameter placeholders.

## Modifying the Lexer

The lexer was my starting point. I needed to teach it to recognise two new patterns: dollar-sign parameters like `$1` and `$name`, and colon parameters like `:name`.

The existing lexer already had a `TokenKind` enumeration that listed all the types of tokens it could recognise. I added a new variant called `NamedParam` for the colon syntax. The dollar-sign syntax could reuse the existing `Variable` token type, but I needed to modify how it was scanned.

For the dollar-sign syntax, the scanning logic needed to handle two cases. If the character after the dollar sign is a digit, we're looking at a numbered parameter like `$1` or `$42`. If it's a letter or underscore, we're looking at a named parameter like `$name` or `$user_id`. The scanning function reads characters until it hits something that isn't part of the parameter name or number.

```rust
fn scan_variable(&mut self) {
    // Skip the '$' symbol
    self.pos += 1;

    if self.source[self.pos].is_ascii_digit() {
        // Numbered parameter: $1, $2, etc.
        // Keep reading while we see digits
        while self.source[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
    } else if self.source[self.pos].is_ascii_alphabetic() {
        // Named parameter: $name
        // Keep reading while we see letters, digits, or underscores
        while self.source[self.pos].is_ascii_alphanumeric()
              || self.source[self.pos] == b'_' {
            self.pos += 1;
        }
    }
}
```

The colon syntax required a bit more care because colons are already used as operators in the language. When the lexer sees a colon, it needs to look ahead at the next character. If that character is a letter or underscore, we're starting a named parameter. Otherwise, it's just a colon operator.

```rust
b':' => {
    // Check if this is a named parameter (:name) or just a colon
    if self.source[self.pos + 1].is_ascii_alphabetic() {
        self.scan_named_param();
        return;
    } else {
        // It's just a colon operator
        TokenKind::Colon
    }
}
```

This lookahead approach ensures backward compatibility - existing queries that use colons for their original purpose continue to work correctly.

## Extending the Abstract Syntax Tree

With the lexer producing parameter tokens, the next step was updating the AST to represent them. The AST already had an `Expr` enumeration for different types of expressions - literals like strings and numbers, identifiers like column names, binary operations like comparisons, and so on.

I added two new variants to this enumeration:

```rust
pub enum Expr {
    Literal(Value),           // Constant values like 42 or "hello"
    Identifier(String),       // Column names like 'id' or 'email'
    NumberedParam(usize),     // $1, $2, etc. - stores the parameter index
    NamedParam(String),       // :name - stores the parameter name
    // ... other variants
}
```

I chose to represent numbered and named parameters as separate variants rather than combining them. This made the code clearer - when processing a `NumberedParam`, I know I'm looking for a value by index; when processing a `NamedParam`, I'm looking for a value by name. The distinction is explicit in the type system.

For numbered parameters, I store the index directly as a `usize`. I made the deliberate choice to use 1-based indexing to match PostgreSQL conventions and user expectations - `$1` refers to the first parameter, not `$0`. The conversion to 0-based indexing for array access happens internally.

## Updating the Parser

The parser's job is to convert tokens into AST nodes. When it encounters a `Variable` token (for dollar-sign syntax) or a `NamedParam` token (for colon syntax), it needs to create the appropriate AST node.

For dollar-sign parameters, the parser extracts the text after the `$` and tries to parse it as a number. If successful, it's a numbered parameter; otherwise, it's a named parameter:

```rust
Some(TokenKind::Variable) => {
    let text = self.token_string(token);  // e.g., "$1" or "$name"
    let param_text = &text[1..];          // Remove the '$' prefix

    if let Ok(num) = param_text.parse::<usize>() {
        // It's a numbered parameter like $1
        if num == 0 {
            return Err("numbered parameters must start at $1");
        }
        Ok(Expr::NumberedParam(num))
    } else {
        // It's a named parameter like $name
        Ok(Expr::NamedParam(param_text.to_string()))
    }
}
```

I added validation to reject `$0` as a parameter. Since I'm using 1-based indexing, allowing `$0` would be confusing and error-prone. It's better to catch this early with a clear error message than to have subtle bugs later.

## Implementing Parameter Resolution in the Executor

The executor is where parameters get their actual values. When the executor encounters a `NumberedParam` or `NamedParam` in the AST, it needs to look up the corresponding value from the parameters that were provided with the query.

I added two new fields to the `QueryExecutor` structure to store parameters:

```rust
pub struct QueryExecutor {
    // ... existing fields ...
    params: Vec<Value>,                    // For numbered parameters
    named_params: BTreeMap<String, Value>, // For named parameters
}
```

The `params` vector stores values in order - the value at index 0 corresponds to `$1`, index 1 to `$2`, and so on. The `named_params` map stores values by name for colon-syntax parameters.

When executing a query, the client provides parameters as a vector of values. If the first value is an object (a key-value map), I extract it into `named_params` for name-based lookups. Otherwise, the values are used positionally for numbered parameters.

```rust
pub fn set_params(&mut self, params: Vec<Value>) {
    self.named_params.clear();

    // If first param is an Object, extract it as named parameters
    if let Some(Value::Object(obj)) = params.first() {
        self.named_params = obj.clone();
    }

    self.params = params;
}
```

The actual parameter resolution happens in the expression evaluation function. When the executor encounters a parameter node in the AST, it looks up the value:

```rust
Expr::NumberedParam(index) => {
    // Convert from 1-indexed to 0-indexed
    let param_index = index - 1;

    self.params.get(param_index)
        .cloned()
        .ok_or_else(|| {
            ExecutorError::InvalidOperation(format!(
                "Parameter ${} not provided", index
            ))
        })
}

Expr::NamedParam(name) => {
    self.named_params.get(&name)
        .cloned()
        .ok_or_else(|| {
            ExecutorError::InvalidOperation(format!(
                "Named parameter '{}' not provided", name
            ))
        })
}
```

I made error handling explicit here. If a query references `$3` but only two parameters were provided, the executor returns a clear error message rather than crashing or returning unexpected results. This helps developers quickly identify and fix issues with their queries.

## Connecting the Client and Server

The protocol that connects clients to the server already included a `params` field in the execute request structure, but it wasn't being used - every request sent an empty parameter array. This was fortunate because it meant I didn't need to change the wire protocol at all, only how it was used.

On the server side, I updated the request handler to extract parameters from the request and pass them to the executor:

```rust
Request::Execute { query, params, .. } => {
    // Set parameters before executing
    executor.set_params(params.clone());

    let result = parse_and_execute(query);

    // Clear parameters after execution
    executor.clear_params();

    result
}
```

I made sure to clear parameters after each query execution. This prevents parameters from accidentally leaking between queries, which could cause subtle bugs if a later query happened to use the same parameter numbers.

On the client side, I added new methods that accept parameters:

```rust
pub async fn query_with_params(
    &self,
    query: impl Into<String>,
    params: Vec<Value>,
) -> Result<QueryResult> {
    let mut conn = self.pool.get().await?;
    let result = conn.execute_with_params(query.into(), params).await?;
    self.pool.return_connection(conn);
    Ok(result)
}
```

The existing `query` and `execute` methods continue to work as before, calling the new methods with an empty parameter vector. This maintains backward compatibility with existing code.

## Testing the Implementation

Testing was crucial for ensuring the implementation worked correctly across different scenarios. I created a dedicated test module covering several categories of tests.

For numbered parameters, I tested basic single-parameter queries, queries with multiple parameters, and error cases where parameters were missing:

```rust
#[tokio::test]
async fn test_numbered_param_single() {
    // Set up test data
    create_test_collection(&mut executor, "users").await;
    insert_record(&mut executor, "users", vec![
        ("id", Value::Int32(1)),
        ("name", Value::String("Alice".to_string())),
    ]).await;

    // Set parameter: $1 = 1
    executor.set_params(vec![Value::Int32(1)]);

    // Query: get from users where id = $1
    let filter = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("id".to_string())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::NumberedParam(1)),
    };

    // Verify we get the expected result
    let results = execute_query(&mut executor, filter).await;
    assert_eq!(results.len(), 1);
}
```

For named parameters, I tested similar scenarios using the colon syntax. I also tested the error handling to ensure meaningful error messages were returned when parameters were missing.

One interesting edge case I discovered during testing was parameter type handling. Parameters can be any value type - integers, strings, floats, booleans, even arrays and objects. The executor needed to handle all these types correctly, passing them through to the filter evaluation without type coercion issues.

## Reflecting on the Design

Looking back at this implementation, I'm satisfied with several design decisions. Supporting both numbered and named parameter syntaxes provides flexibility without adding significant complexity. The clear separation between the lexer, parser, and executor made modifications straightforward - each component had a well-defined responsibility.

One area where I made a conscious trade-off was in how named parameters are passed. Rather than adding a separate parameter to the protocol for named parameters, I chose to pack them into an object value within the existing params array. This kept the protocol simpler but means clients need to construct the object themselves. In hindsight, having explicit support for named parameters in the protocol might have been cleaner.

The implementation also laid groundwork for future enhancements. With parameters separated from queries, it becomes possible to implement prepared statements - pre-parsing a query once and executing it multiple times with different parameters. This could provide significant performance benefits for frequently-executed queries.

## Conclusion

Implementing query parameters touched every layer of the MonoDB query pipeline, from lexical analysis through to execution. The feature required careful consideration of syntax design, error handling, and backward compatibility. The result is a more robust and flexible query system that follows established database conventions, making MonoDB queries both safer and more efficient.

The numbered parameter syntax (`$1`, `$2`) provides a familiar PostgreSQL-like experience for simple queries, while named parameters (`:name`) offer improved readability for complex queries with many parameters. Together, they give developers the tools they need to write clean, maintainable database code.
