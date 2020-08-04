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

// Short-hand for a list of tables all inner-joined.
pub struct TableList(pub Vec<TableOrSubquery>);

impl TableList {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

pub struct Join {
    left: TableOrSubquery,
    op: JoinOp,
    right: TableOrSubquery,
    // TODO: constraints (ON, USING).
}

#[allow(dead_code)]
pub enum TableOrSubquery {
    Table(SourceAlias),
    Union(Vec<SelectQuery>, TableAlias),
    Subquery(Box<SelectQuery>),
    Values(Values, TableAlias),
}

pub enum Values {
    /// Like "VALUES (0, 1), (2, 3), ...".
    /// The vector must be of a length that is a multiple of the given size.
    Unnamed(usize, Vec<TypedValue>),

    /// Like "SELECT 0 AS x, SELECT 0 AS y WHERE 0 UNION ALL VALUES (0, 1), (2, 3), ...".
    /// The vector of values must be of a length that is a multiple of the length
    /// of the vector of names.
    Named(Vec<Variable>, Vec<TypedValue>),
}
