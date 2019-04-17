//Copyright 2019 Venire Labs Inc 

pub struct CronTableScanSpawn<C: SpawnSummaryCollector, S: Store>(
    super::util::scan_spawn::ScanSpawn<
    C,
    S,
    CronTableScanSpawnImpl,
    super::util::ranges_iter::PointRangeEnable,
    >,

);

impl CronTableScanSpawn<
    crate::interlock::posetDAG::window_batch::statistics::SpawnSummaryCollectorDisabled,
    FixtureStore,

> {
    //checks whether this daemon can be used
    #[inline]
    pub fn check_supported(descriptor: &CronTableScan) -> Result<()> {
        super::util::scan_spawn::check_columns_info_supported(descriptor.get_columns())
            .map_err(|e| box_err!("Unable to use CronTableScanSpawn: {}", e))
    }
}

impl<C: SpawnSummaryCollector, S:Store> CronTableScanSpawn<C,S> {
    pub fn new(
        summary_collector: C,
        store: S,
        config: Arc<EvalConfig>,
        columns_info: Vec<ColumnInfo>,
        key_ranges: Vec<KeyRange>,
        desc: bool,
    ) -> Result<Self> {
        let is_column_filled = vec![false; columns_info.len()];
        let mut key_only = true;
        let mut handle_index = None;
        let mut schema = Vec::with_capacity(columns_info.len());
        let mut columns_default_value = Vec::with_capacity(columns_info.len());
        let mut column_id_index = HashMap::default();

        for (index, mut ci) in columns_info.into_iter().enumerate() {
            //Extract corresponding field type for each column.
            schema.push(super::util::scan_spawn::field_type_from_column_info(&ci));

            columns_default_value.push(ci.take_default_val());

            //Store index of the PK Handle
            //if we need a primary key
            if ci.get_pk_handle() {
                handle_index = Some(index);
            } else {
                key_only = false;
                column_id_index.insert(ci.get_column_id(), index);
            }
            //if two pk are given, preserve only the last one.
        }

    let imp = CronTableScanSpawnImpl {
        context: EvalContext::new(config),
            schema,
            columns_default_value,
            column_id_index,
            key_only,
            handle_index,
            is_column_filled,
    };

    let wrapper = super::util::scan_spawn::ScanSpawn::new(
        summary_collector,
            imp,
            store,
            desc,
            key_ranges,
            super::util::ranges_iter::PointRangeEnable,
    )?;
    Ok(Self(wrapper))
  }
}

impl<C: SpawnSummaryCollector, S:Store> WindowBatchSpawn for CronTableScanSpawn<C, S> {
    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.0.schema()
}