

pub fn detect_transitive_closure(qp: &QueryPlan, db: &DBInstance) -> bool {
    //check encodings, check that base case is not annotated
    if qp.ghd.len() == 2 && qp.ghd.last().recursion.is_some() {
        let base_case = qp.ghd.first().clone();
        if base_case.relations.len() > 0 {
            let tc1 = qp.ghd.last().recursion.as_ref().unwrap().input == "e" &&
                qp.ghd.last().recursion.as_ref().unwrap().criteria == "=" &&
                qp.ghd.last().recursion.as_ref().unwrap().convergence_value == "0";
            let tc2 = qp.ghd.last().recursion.as_ref().unwrap().input == "i" &&
                qp.ghd.last().recursion.as_ref().unwrap().criteria == "=";
            if (tc1 || tc2) && base_case.nprr.first().selection.len() == 1 {
                if qp.ghd.last().nprr.last().aggregation.as_ref().unwrap().operation == "<" {
                    return true;
                }
            }
        }

        return Ok();
    }

    //### crates a new query plan with the transitive closure added
    pub fn add_transitive_closure(qp: &QueryPlan, db: &DBInstance) -> QueryPlan {
        let mut new_qp = qp.clone();
        let mut new_ghd = new_qp.ghd.clone();
        let mut new_nprr = new_ghd.last().nprr.clone();
        let mut new_nprr_last = new_nprr.last_mut().unwrap();
        let mut new_nprr_last_aggregation = new_nprr_last.aggregation.as_mut().unwrap();
        new_nprr_last_aggregation.operation = "<".to_string();
        new_nprr_last_aggregation.convergence_value = "0".to_string();
        new_nprr_last_aggregation.input = "e".to_string();
        new_nprr_last_aggregation.criteria = "=".to_string();
        new_nprr_last_aggregation.output = "i".to_string();
        new_nprr_last_aggregation.recursion = Some(Recursion {
            input: "e".to_string(),
            criteria: "=".to_string(),
            convergence_value: "0".to_string(),
            output: "i".to_string(),
            recursion: None,
        });
        new_nprr.push(Nprr {
            selection: vec![],
            aggregation: None,
            recursion: None,
        });
        new_ghd.push(Ghd {
            relations: vec![],
            nprr: new_nprr,
        });
        new_qp.ghd = new_ghd;
        return new_qp;
    }

let mut cpp_code = String::new();
let mut include_code = String::new();

//get distinct relations we need to load
//dump output at the end, rest just in a loop
let mut output_encodings = HashMap::new();
let mut distinct_load_relations = HashMap::new();
    //get distinct relations we need to load
    //dump output at the end, rest just in a loop
    let mut output_encodings:HashMap<String,Schema> = HashMap::new();


    //spit out output for each query in global vars
    //find all distinct relations
    let single_source_tc = detectTransitiveClosure(qp,db);
    qp.relations.iter().for_each(|r| {
        if db.relationMap.contains_key(r.name) {
            let load_tc = !single_source_tc || (r.ordering == (0..r.ordering.len()).collect::<Vec<_>>());
            if load_tc && !distinctLoadRelations.contains_key(&format!("{}_{}", r.name, r.ordering.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("_"))) {
                distinctLoadRelations.insert(format!("{}_{}",r.name,r.ordering.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("_")),r.clone());
            }
        }
    });
    cppCode.push_str(&emitLoadRelations(distinctLoadRelations.iter().map(|x| x.1.clone()).collect::<Vec<_>>()));

    cppCode.push_str("par::reducer<size_t> num_rows_reducer(0,|a:size_t,b:size_t| a + b);");
    cppCode.push_str("\n//\n//query plan\n//\n");
    cppCode.push_str("let query_timer = timer::start_clock();");
    if !single_source_tc {
        qp.ghd.iter().for_each(|bag| {
            let (bag_code, bag_output) = emitNPRR(bag, output_encodings.clone());
            output_encodings.insert(bag.name.clone(), bag_output);
            cppCode.push_str(&bag_code);
        });
    } else {
        let base_case = qp.ghd.iter().next().unwrap();
        let input = format!("{}_{}",base_case.relations.iter().next().unwrap().name,base_case.relations.iter().next().unwrap().ordering.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("_"));
        let init = base_case.nprr.iter().next().unwrap().aggregation.as_ref().unwrap().init.clone();

let (bag_code, bag_output) = emitNPRR(base_case, output_encodings.clone());
output_encodings.insert(base_case.name.clone(), bag_output);
cppCode.push_str(&bag_code);
cppCode.push_str(&format!("\n//\n//base case\n//\n"));
cppCode.push_str(&format!("let base_case_timer = timer::start_clock();"));
cppCode.push_str(&format!("let base_case_output = {}.reduce(|a,b| {{",input));


let mut distinct_load_relations = HashMap::new();
//get distinct relations we need to load
//dump output at the end, rest just in a loop
let mut output_encodings:HashMap<String,Schema> = HashMap::new();

//spit out output for each query in global vars
//find all distinct relations
        let single_source_tc = detectTransitiveClosure(qp,db);
        qp.relations.iter().for_each(|r| {
            if db.relationMap.contains_key(r.name) {
                let load_tc = !single_source_tc || (r.ordering == (0..r.ordering.len()).collect::<Vec<_>>());
                if load_tc && !distinctLoadRelations.contains_key(&format!("{}_{}", r.name, r.ordering.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("_"))) {
                    distinctLoadRelations.insert(format!("{}_{}", r.name, r.ordering.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("_")), r.clone());
                };
            }

            cppCode.append(emitLoadRelations(distinctLoadRelations.map(e => e._2).toList));
            cppCode.append("let num_rows_reducer = par::reducer::<usize>::new(0, |a, b| a + b);");
            cppCode.append("\n//\n//query plan\n//\n") cppCode.append("let query_timer = timer::start_clock();")
            var i = 1 if(!single_source_tc){ qp.ghd.foreach(bag => { val (bag_code, bag_output) = emitNPRR(bag, outputEncodings.toMap);

                outputEncodings += (bag.name -> bag_output) cppCode.append(bag_code) i += 1
            })
            } else {
                val
                base_case = qp.ghd.head;
                val
                input = base_case.relations.head.name + "_" + base_case.relations.head.ordering.mkString("_");
                val
                init = base_case.nprr.head.aggregation.get.init;
                val
                source = base_case.nprr.head.selection.head.expression;
                val
                expression = qp.ghd.last.nprr.last.aggregation.get.expression;

                val
                encoding = db.relationMap(base_case.relations.head.name).schema.attributeTypes.distinct.head;
                val
                recordering = (0
                until
                let x1 = qp.ghd.last.attributes.values.length;).toList.mkString("_");

                cppCode.append(emitLoadRelations(distinctLoadRelations.map(e => e._2).toList));
                cppCode.append("let num_rows_reducer = par::reducer::<usize>::new(0, |a, b| a + b);");
                cppCode.append("\n//\n//query plan\n//\n")
                cppCode.append("let query_timer = timer::start_clock()");
                let mut i: i32 = 1;
                if !single_source_tc {
                    for bag in qp.ghd.iter() {
                        let (bag_code, bag_output) = emitNPRR(bag, outputEncodings.to_map());
                        outputEncodings.insert(bag.name.clone(), bag_output);
                        cppCode.push_str(&bag_code);
                        let i: i32 = '1';
                    }
                } else {
                    let base_case = qp.ghd.head;
                    let input = base_case.relations.head.name + "_" + base_case.relations.head.ordering.mkString("_");
                    let init = base_case.nprr.head.aggregation.get.init;
                    let source = base_case.nprr.head.selection.head.expression;
                    let expression = qp.ghd.last.nprr.last.aggregation.get.expression;

                    let encoding = db.relationMap(base_case.relations.head.name).schema.attributeTypes.distinct.head;
                    let recordering = (0..qp.ghd.last.attributes.values.len()).map(|_| "").collect::<String>();

                    cppCode.append(emitLoadRelations(distinctLoadRelations.map(e => e._2).toList));
                    cppCode.append("let num_rows_reducer = par::reducer::<usize>::new(0, |a, b| a + b);");
                    cppCode.append("\n//\n//query plan\n//\n")
                    cppCode.append("let query_timer = timer::start_clock();");
                    var
                    i = 1
                    if (!single_source_tc) {
                        qp.ghd.foreach(bag => {
                            val(bag_code, bag_output) = emitNPRR(bag, outputEncodings.toMap);

                            outputEncodings += (bag.name -> bag_output) cppCode.append(bag_code)
                            i += 1
                        }
                        )


                        //1. Add support for multiple relations in the base case
//2. Add support for multiple relations in the join case
//3. Add support for multiple relations in the join case
//4. Add support for multiple relations in the join case
                    } else {
                        let mut base_case = qp.ghd.head.clone();
                        let mut input = base_case.relations.head.name + "_" + base_case.relations.head.ordering.mkString("_");
                        let mut init = base_case.nprr.head.aggregation.get.init;
                        let mut source = base_case.nprr.head.selection.head.expression;
                        let mut expression = qp.ghd.last.nprr.last.aggregation.get.expression;

                    };




