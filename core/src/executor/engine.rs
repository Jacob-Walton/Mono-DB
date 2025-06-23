use super::*;
use crate::nsql::ast::*;
use crate::nsql::interner::StringInterner;
use std::collections::HashMap;

pub struct Executor;

impl Executor {
    pub fn new() -> Self {
        Self
    }

    pub fn execute_program(
        &self,
        program: &Program,
        ctx: &mut ExecutionContext,
    ) -> ExecutionResult<Vec<QueryResult>> {
        let mut results = Vec::new();

        for statement in &program.statements {
            let result = self.execute_statement(statement, ctx)?;
            results.push(result);
        }

        Ok(results)
    }

    pub fn execute_statement(
        &self,
        statement: &Statement,
        ctx: &mut ExecutionContext,
    ) -> ExecutionResult<QueryResult> {
        match statement {
            Statement::Query(query) => self.execute_query(query, ctx),
            Statement::Insert(insert) => self.execute_insert(insert, ctx),
            Statement::CreateTable(create) => self.execute_create_table(create, ctx),
            Statement::DropTable(drop) => self.execute_drop_table(drop, ctx),
            Statement::Update(update) => self.execute_update(update, ctx),
            Statement::Delete(delete) => self.execute_delete(delete, ctx),
        }
    }

    fn execute_query(
        &self,
        query: &Query,
        ctx: &mut ExecutionContext,
    ) -> ExecutionResult<QueryResult> {
        // Get source data
        let mut rows = if let Some(from_table) = &query.from {
            self.scan_table(from_table, ctx)?
        } else {
            // SELECT without FROM (e.g., SELECT 1, 'hello')
            vec![Row::new()]
        };

        // Apply WHERE clause
        if let Some(where_expr) = &query.where_clause {
            rows = self.apply_where_clause(rows, where_expr, ctx)?;
        }

        // Apply projection
        rows = self.apply_projection(&query.projection, rows, ctx)?;

        Ok(QueryResult::rows(rows))
    }

    fn scan_table(
        &self,
        table_name: &QualifiedName,
        ctx: &mut ExecutionContext,
    ) -> ExecutionResult<Vec<Row>> {
        let table_str = self.qualified_name_to_string(table_name, &ctx.interner);

        // Get table schema first to validate table exists
        let _schema = ctx.storage.get_table_schema(&table_str)?;

        // Now scan the table
        let rows_data = ctx.storage.scan_table(&table_str)?;
        let mut rows = Vec::new();

        for row_data in rows_data {
            let mut row = Row::new();
            for (column, value) in row_data {
                row.insert(column, value);
            }
            rows.push(row);
        }

        Ok(rows)
    }

    fn apply_where_clause(
        &self,
        rows: Vec<Row>,
        where_expr: &Expression,
        ctx: &mut ExecutionContext,
    ) -> ExecutionResult<Vec<Row>> {
        let mut filtered_rows = Vec::new();

        for row in rows {
            if self.evaluate_expression_bool(where_expr, &row, ctx)? {
                filtered_rows.push(row);
            }
        }

        Ok(filtered_rows)
    }

    fn apply_projection(
        &self,
        projection: &[SelectItem],
        rows: Vec<Row>,
        ctx: &mut ExecutionContext,
    ) -> ExecutionResult<Vec<Row>> {
        let mut projected_rows = Vec::new();

        for row in rows {
            let mut new_row = Row::new();

            for item in projection {
                match &item.expr {
                    Expression::Star => {
                        // Include all columns from the row
                        for (column, value) in &row.data {
                            new_row.insert(column.clone(), value.clone());
                        }
                    }
                    _ => {
                        let value = self.evaluate_expression(&item.expr, &row, ctx)?;
                        let column_name = if let Some(alias) = &item.alias {
                            ctx.interner
                                .resolve(alias.0)
                                .unwrap_or("unnamed")
                                .to_string()
                        } else {
                            self.infer_column_name(&item.expr, ctx)
                        };

                        new_row.insert(column_name, value);
                    }
                }
            }

            projected_rows.push(new_row);
        }

        Ok(projected_rows)
    }

    fn execute_insert(
        &self,
        insert: &InsertStmt,
        ctx: &mut ExecutionContext,
    ) -> ExecutionResult<QueryResult> {
        let table_name = self.qualified_name_to_string(&insert.table, &ctx.interner);
        let mut rows_affected = 0;

        for row_values in &insert.values {
            let mut row_data = HashMap::new();

            if let Some(columns) = &insert.columns {
                if columns.len() != row_values.len() {
                    return Err(ExecutionError::InvalidExpression(
                        "Column count mismatch".to_string(),
                    ));
                }

                for (i, column) in columns.iter().enumerate() {
                    let column_name = ctx.interner.resolve(column.0).unwrap().to_string();
                    let value = self.evaluate_expression(&row_values[i], &Row::new(), ctx)?;
                    row_data.insert(column_name, value);
                }
            } else {
                // Use table schema column order
                let schema = ctx.storage.get_table_schema(&table_name)?;
                if schema.len() != row_values.len() {
                    return Err(ExecutionError::InvalidExpression(
                        "Column count mismatch".to_string(),
                    ));
                }

                for (i, column_info) in schema.iter().enumerate() {
                    let value = self.evaluate_expression(&row_values[i], &Row::new(), ctx)?;
                    row_data.insert(column_info.name.clone(), value);
                }
            }

            ctx.storage.insert_row(&table_name, row_data)?;
            rows_affected += 1;
        }

        Ok(QueryResult::affected(rows_affected))
    }

    fn execute_create_table(
        &self,
        create: &CreateTableStmt,
        ctx: &mut ExecutionContext,
    ) -> ExecutionResult<QueryResult> {
        let table_name = self.qualified_name_to_string(&create.name, &ctx.interner);

        let mut columns = Vec::new();
        for column_def in &create.columns {
            let column_name = ctx.interner.resolve(column_def.name.0).unwrap().to_string();
            let nullable = !column_def.constraints.contains(&ColumnConstraint::NotNull);

            columns.push(ColumnInfo {
                name: column_name,
                data_type: column_def.data_type.clone(),
                nullable,
            });
        }

        ctx.storage.create_table(&table_name, &columns)?;
        Ok(QueryResult::created())
    }

    fn execute_drop_table(
        &self,
        drop: &DropTableStmt,
        ctx: &mut ExecutionContext,
    ) -> ExecutionResult<QueryResult> {
        let table_name = self.qualified_name_to_string(&drop.name, &ctx.interner);

        ctx.storage.drop_table(&table_name)?;
        Ok(QueryResult::affected(1))
    }

    fn execute_update(
        &self,
        update: &UpdateStmt,
        ctx: &mut ExecutionContext,
    ) -> ExecutionResult<QueryResult> {
        let table_name = self.qualified_name_to_string(&update.table, &ctx.interner);
        let rows_data = ctx.storage.scan_table(&table_name)?;
        let schema = ctx.storage.get_table_schema(&table_name)?;
        let mut rows_affected = 0;

        // Clear and recreate table
        ctx.storage.drop_table(&table_name)?;
        ctx.storage.create_table(&table_name, &schema)?;

        for mut row_data in rows_data {
            let row = Row {
                data: row_data.clone(),
            };

            let should_update = if let Some(where_expr) = &update.where_clause {
                self.evaluate_expression_bool(where_expr, &row, ctx)?
            } else {
                true
            };

            if should_update {
                // Apply assignments
                for assignment in &update.assignments {
                    let column_name = ctx
                        .interner
                        .resolve(assignment.column.0)
                        .unwrap()
                        .to_string();
                    let new_value = self.evaluate_expression(&assignment.value, &row, ctx)?;
                    row_data.insert(column_name, new_value);
                }
                rows_affected += 1;
            }

            ctx.storage.insert_row(&table_name, row_data)?;
        }

        Ok(QueryResult::affected(rows_affected))
    }

    fn execute_delete(
        &self,
        delete: &DeleteStmt,
        ctx: &mut ExecutionContext,
    ) -> ExecutionResult<QueryResult> {
        let table_name = self.qualified_name_to_string(&delete.table, &ctx.interner);
        let rows_data = ctx.storage.scan_table(&table_name)?;
        let schema = ctx.storage.get_table_schema(&table_name)?;
        let mut rows_affected = 0;

        // Clear and recreate table
        ctx.storage.drop_table(&table_name)?;
        ctx.storage.create_table(&table_name, &schema)?;

        for row_data in rows_data {
            let row = Row {
                data: row_data.clone(),
            };

            let should_delete = if let Some(where_expr) = &delete.where_clause {
                self.evaluate_expression_bool(where_expr, &row, ctx)?
            } else {
                true
            };

            if should_delete {
                rows_affected += 1;
                // Don't re-insert this row
            } else {
                // Re-insert this row
                ctx.storage.insert_row(&table_name, row_data)?;
            }
        }

        Ok(QueryResult::affected(rows_affected))
    }

    // Expression evaluation
    fn evaluate_expression(
        &self,
        expr: &Expression,
        row: &Row,
        ctx: &mut ExecutionContext,
    ) -> ExecutionResult<Value> {
        match expr {
            Expression::Literal(literal) => self.evaluate_literal(literal, ctx),
            Expression::Column(col_name) => {
                let column_str = self.qualified_name_to_string(col_name, &ctx.interner);
                row.get(&column_str)
                    .cloned()
                    .ok_or_else(|| ExecutionError::ColumnNotFound(column_str))
            }
            Expression::Star => Err(ExecutionError::InvalidExpression(
                "Cannot evaluate * as a value".to_string(),
            )),
            Expression::BinaryOp { op, left, right } => {
                let left_val = self.evaluate_expression(left, row, ctx)?;
                let right_val = self.evaluate_expression(right, row, ctx)?;
                self.apply_binary_op(*op, left_val, right_val)
            }
        }
    }

    fn evaluate_literal(
        &self,
        literal: &Literal,
        ctx: &mut ExecutionContext,
    ) -> ExecutionResult<Value> {
        match literal {
            Literal::Integer(i) => Ok(Value::Integer(*i)),
            Literal::Boolean(b) => Ok(Value::Boolean(*b)),
            Literal::Null => Ok(Value::Null),
            Literal::String(s) => {
                let string_val = ctx.interner.resolve(*s).unwrap_or("").to_string();
                Ok(Value::String(string_val))
            }
        }
    }

    fn apply_binary_op(&self, op: BinaryOp, left: Value, right: Value) -> ExecutionResult<Value> {
        match op {
            BinaryOp::Eq => Ok(Value::Boolean(self.values_equal(&left, &right))),
            BinaryOp::NotEq => Ok(Value::Boolean(!self.values_equal(&left, &right))),
            BinaryOp::Lt => {
                self.compare_values(&left, &right, |ord| ord == std::cmp::Ordering::Less)
            }
            BinaryOp::Gt => {
                self.compare_values(&left, &right, |ord| ord == std::cmp::Ordering::Greater)
            }
            BinaryOp::LtEq => {
                self.compare_values(&left, &right, |ord| ord != std::cmp::Ordering::Greater)
            }
            BinaryOp::GtEq => {
                self.compare_values(&left, &right, |ord| ord != std::cmp::Ordering::Less)
            }
            BinaryOp::And => {
                let left_bool = self.value_to_bool(&left)?;
                let right_bool = self.value_to_bool(&right)?;
                Ok(Value::Boolean(left_bool && right_bool))
            }
            BinaryOp::Or => {
                let left_bool = self.value_to_bool(&left)?;
                let right_bool = self.value_to_bool(&right)?;
                Ok(Value::Boolean(left_bool || right_bool))
            }
        }
    }

    fn values_equal(&self, left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => l == r,
            (Value::Float(l), Value::Float(r)) => l == r,
            (Value::String(l), Value::String(r)) => l == r,
            (Value::Boolean(l), Value::Boolean(r)) => l == r,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }

    fn compare_values<F>(&self, left: &Value, right: &Value, cmp_fn: F) -> ExecutionResult<Value>
    where
        F: Fn(std::cmp::Ordering) -> bool,
    {
        let ordering = match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => l.cmp(r),
            (Value::Float(l), Value::Float(r)) => {
                l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::String(l), Value::String(r)) => l.cmp(r),
            (Value::Boolean(l), Value::Boolean(r)) => l.cmp(r),
            _ => {
                return Err(ExecutionError::TypeMismatch {
                    expected: "comparable types".to_string(),
                    actual: "incompatible types".to_string(),
                });
            }
        };

        Ok(Value::Boolean(cmp_fn(ordering)))
    }

    fn value_to_bool(&self, value: &Value) -> ExecutionResult<bool> {
        match value {
            Value::Boolean(b) => Ok(*b),
            Value::Integer(i) => Ok(*i != 0),
            Value::Null => Ok(false),
            _ => Err(ExecutionError::TypeMismatch {
                expected: "boolean".to_string(),
                actual: "other".to_string(),
            }),
        }
    }

    fn evaluate_expression_bool(
        &self,
        expr: &Expression,
        row: &Row,
        ctx: &mut ExecutionContext,
    ) -> ExecutionResult<bool> {
        let value = self.evaluate_expression(expr, row, ctx)?;
        self.value_to_bool(&value)
    }

    fn infer_column_name(&self, expr: &Expression, ctx: &ExecutionContext) -> String {
        match expr {
            Expression::Column(col_name) => self.qualified_name_to_string(col_name, &ctx.interner),
            Expression::Literal(Literal::Integer(i)) => format!("{}", i),
            Expression::Literal(Literal::String(s)) => {
                ctx.interner.resolve(*s).unwrap_or("string").to_string()
            }
            Expression::Literal(Literal::Boolean(b)) => format!("{}", b),
            Expression::Literal(Literal::Null) => "null".to_string(),
            Expression::Star => "*".to_string(),
            Expression::BinaryOp { .. } => "expr".to_string(),
        }
    }

    fn qualified_name_to_string(&self, name: &QualifiedName, interner: &StringInterner) -> String {
        name.parts
            .iter()
            .map(|part| interner.resolve(*part).unwrap_or("unknown"))
            .collect::<Vec<_>>()
            .join(".")
    }
}
