pub enum ColumnOrExpression {
    Column(QualifiedAlias),
    ExistingColumn(Name),
    Causetid(Causetid),       // Because it's so common.
    Integer(i32),       // We use these for type codes etc.
    Long(i64),
    Value(TypedValue),
    // Some aggregates (`min`, `max`, `avg`) can be over 0 rows, and therefore can be `NULL`; that
    // needs special treatment.
    NullableAggregate(Box<Expression>, ValueType),      // Track the return type.
    Expression(Box<Expression>, ValueType),             // Track the return type.
}

pub enum Expression {
    Unary { sql_op: &'static str, arg: ColumnOrExpression },
}

/// `QueryValue` and `ColumnOrExpression` are almost identical… merge somehow?
impl From<QueryValue> for ColumnOrExpression {
    fn from(v: QueryValue) -> Self {
        match v {
            QueryValue::Column(c) => ColumnOrExpression::Column(c),
            QueryValue::Causetid(e) => ColumnOrExpression::Causetid(e),
            QueryValue::PrimitiveLong(v) => ColumnOrExpression::Long(v),
            QueryValue::TypedValue(v) => ColumnOrExpression::Value(v),
        }
    }
}