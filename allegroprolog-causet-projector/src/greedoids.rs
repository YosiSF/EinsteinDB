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