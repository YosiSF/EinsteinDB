use std::fs::File;
use std::io::prelude::*;

#[derive(Default, Debug)]
struct TestCase {
    pub var_names: Vec<String>,

    pub inputs: Vec<Vec<i32>>,
    pub outputs: Vec<Vec<i32>>,

    pub queries: Vec<Vec<i32>>,
    pub answers: Vec<Vec<i32>>,
}


fn main() {
    let mut f = File::open("einstein_db/tests/rust/test_cases.json").unwrap();
    let mut contents = String::new();
    f.read_to_string(&mut contents).unwrap();
    let test_cases: Vec<TestCase> = serde_json::from_str(&contents).unwrap();

    for test_case in test_cases {
        let mut db = EinsteinDB::new();
        for (var_name, var_value) in test_case.var_names.iter().zip(test_case.inputs.iter()) {
            db.set_var(var_name, var_value.clone());
        }

        for (query, answer) in test_case.queries.iter().zip(test_case.answers.iter()) {
            let result = db.query(query);
            assert_eq!(result, answer);
        }
    }
}


#[derive(Default, Debug)]
struct EinsteinDB {
    var_names: Vec<String>,
    var_values: Vec<Vec<i32>>,
}


