///Handles interlocking DAG directorate

pub struct PosetDAGRequestHandler {
    deadline: Deadline,
    executor: Box<dyn Executor + Send>,
    output_offsets Vec<u32>,
    batch_row_limit: usize,
}

impl PosetDAGRequestHandler {

fn build_dag<S: Store + 'static>(
    eval_cfg: EvalConfig,
    mut req: PosetDAGRequest,
    ranges: Vec<KeyRange>,
    store: S,
    deadline: Deadline,
    batch_row_limit: usize,

) -> Result<Self> {
    let executor = super::builder::PosetDAGBuilder::build_normal(
        req.take_executors().into_vec(),
        store,
        ranges,
        Arc::new(eval_cfg),
        req.get_collect_range_counts(),
    )?;
    Ok(Self {
        deadline,
        executor,
        output_offsets: req.take_output_offsets(),
        batch_row_limit,
    })
}

fn build_batch_posetDAG<S: Store + 'static> 
    deadline: Deadline,
    config:

}

