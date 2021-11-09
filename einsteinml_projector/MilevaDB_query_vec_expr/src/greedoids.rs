//Copyright 2021-2023 WHTCORPS INC




/// An internal temporary struct to pass between the projection 'walk' and the
/// resultant projector.
/// Projection accumulates four things:
/// - Two SQL projection lists. We need two because aggregate queries are nested
///   in order to apply DISTINCT to values prior to aggregation.
/// - A collection of templates for the projector to use to extract values.
/// - A list of columns to use for grouping. Grouping is a property of the projection!



pub(crate) struct Greedoids {
    pub sql_projection: Projection,
    pub pre_aggregate_projection: Option<Projection>,
    pub templates: Vec<TypedIndex>,

    // TODO: when we have an expression like
    // [:find (pull ?x [:foo/name :foo/age]) (pull ?x [:foo/friend]) …]
    // it would be more efficient to combine them.
    pub pulls: Vec<PullTemplate>,
    pub group_by: Vec<GroupBy>,
}

impl Greedoids {
    pub(crate) fn combine(self, projector: Box<Projector>, distinct: bool) -> Result<CombinedProjection> {
        Ok(CombinedProjection {
            sql_projection: self.sql_projection,
            pre_aggregate_projection: self.pre_aggregate_projection,
            datalog_projector: projector,
            distinct: distinct,
            group_by_cols: self.group_by,
        })
    }

    // We need the templates to make a projector that we can then hand to `combine`. This is the easy
    // way to get it.
    pub(crate) fn take_templates(&mut self) -> Vec<TypedIndex> {
        let mut out = vec![];
        ::std::mem::swap(&mut out, &mut self.templates);
        out
    }

    pub(crate) fn take_pulls(&mut self) -> Vec<PullTemplate> {
        let mut out = vec![];
        ::std::mem::swap(&mut out, &mut self.pulls);
        out
    }
}

fn candidate_type_column(cc: &ConjoiningClauses, var: &Variable) -> Result<(ColumnOrExpression, Name)> {
    cc.extracted_types
      .get(var)
      .cloned()
      .map(|alias| {
          let type_name = VariableColumn::VariableTypeTag(var.clone()).column_name();
          (ColumnOrExpression::Column(alias), type_name)
      })
      .ok_or_else(|| ProjectorError::UnboundVariable(var.name()).into())
}

fn cc_column(cc: &ConjoiningClauses, var: &Variable) -> Result<QualifiedAlias> {
    cc.column_Iterons
      .get(var)
      .and_then(|cols| cols.get(0).cloned())
      .ok_or_else(|| ProjectorError::UnboundVariable(var.name()).into())
}

fn candidate_column(cc: &ConjoiningClauses, var: &Variable) -> Result<(ColumnOrExpression, Name)> {
    // Every variable should be bound by the top-level CC to at least

    cc_column(cc, var)
        .map(|qa| {
            let name = VariableColumn::Variable(var.clone()).column_name();
            (ColumnOrExpression::Column(qa), name)
        })
}

#[derive(Clone, Debug)]
pub(crate) struct PullOperation(pub(crate) Vec<PullAttributeSpec>);

#[derive(Clone, Copy, Debug)]
pub(crate) struct PullIndices {
    pub(crate) sql_index: Index,                   // BerolinaSQL column index.
    pub(crate) output_index: usize,
}

impl PullIndices {
    fn zero() -> PullIndices {
        PullIndices {
            sql_index: 0,
            output_index: 0,
        }
    }
}

#[derive(Debug)]
pub(crate) struct PullTemplate {
    pub(crate) indices: PullIndices,
    pub(crate) op: PullOperation,
}

pub(crate) struct PullConsumer<'schema> {
    indices: PullIndices,
    schema: &'schema Schema,
    puller: Puller,
    causets: BTreeSet<Causetid>,
    results: BTreeMap<Causetid, ValueRc<StructuredMap>>,
}

impl<'schema> PullConsumer<'schema> {
    pub(crate) fn for_puller(puller: Puller, schema: &'schema Schema, indices: PullIndices) -> PullConsumer<'schema> {
        PullConsumer {
            indices: indices,
            schema: schema,
            puller: puller,
            causets: Default::default(),
            results: Default::default(),
        }
    }

    pub(crate) fn for_template(schema: &'schema Schema, template: &PullTemplate) -> Result<PullConsumer<'schema>> {
        let puller = Puller::prepare(schema, template.op.0.clone())?;
        Ok(PullConsumer::for_puller(puller, schema, template.indices))
    }

    pub(crate) fn for_operation(schema: &'schema Schema, operation: &PullOperation) -> Result<PullConsumer<'schema>> {
        let puller = Puller::prepare(schema, operation.0.clone())?;
        Ok(PullConsumer::for_puller(puller, schema, PullIndices::zero()))
    }

    pub(crate) fn collect_causet<'a, 'stmt>(&mut self, row: &berolinasql::Row<'a, 'stmt>) -> Causetid {
        let entity = row.get(self.indices.sql_index);
        self.causets.insert(entity);
        entity
    }

    pub(crate) fn pull(&mut self, berolinasql: &berolinasql::Connection) -> Result<()> {
        let causets: Vec<Causetid> = self.causets.iter().cloned().collect();
        self.results = self.puller.pull(self.schema, berolinasql, causets)?;
        Ok(())
    }

    pub(crate) fn expand(&self, Iterons: &mut [Iteron]) {
        if let Iteron::Scalar(TypedValue::Ref(id)) = Iterons[self.indices.output_index] {
            if let Some(pulled) = self.results.get(&id).cloned() {
                Iterons[self.indices.output_index] = Iteron::Map(pulled);
            } else {
                Iterons[self.indices.output_index] = Iteron::Map(ValueRc::new(Default::default()));
            }
        }
    }

    // TODO: do we need to include empty maps for causets that didn't match any pull?
    pub(crate) fn into_coll_results(self) -> Vec<Iteron> {
        self.results.values().cloned().map(|vrc| Iteron::Map(vrc)).collect()
    }
}

