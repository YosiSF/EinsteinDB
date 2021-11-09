/* Here's what the below class is doing:
1. It first defines an empty statement, which is just an empty string.
2. It then defines an identifier name, which is a string that starts with a letter or underscore, and is followed by any number of letters, numbers, or underscores.
3. It then defines an attribute list, which is a comma-separated list of identifier names.
4. It then defines a not last attribute, which is an identifier name followed by a comma and an attribute list.
5. It then defines a last attribute, which is just an identifier name.
*/




impl DatalogParser {
    pub fn run(&self, line: &str) -> IR {
        let mut irbuilder = IRBuilder::new();
        let parsed_rules = self.rule(line);
        match parsed_rules {
            Ok(parsed_rules) => {
                irbuilder.add_rules(parsed_rules);
            }
            Err(e) => {
                panic!("{}", e);
            }
        }
        irbuilder.build()
    }

    fn empty_statement(&self) -> String {
        "".to_string()
    }

    fn identifier_name(&self) -> String {
        r"[_\p{L}][_\p{L}\p{Nd}]*".to_string()
    }

    fn attr_list(&self) -> Vec<String> {
        let mut attr_list = Vec::new();
        let not_last_attr = self.not_last_attr();
        let last_attr = self.last_attr();
        let empty_statement = self.empty_statement();
        let attr_list_result = self.attr_list_result(not_last_attr, last_attr, empty_statement);
        for attr in attr_list_result {
            attr_list.push(attr);
        }
        attr_list
    }

    fn not_last_attr(&self) -> String {
        let identifier_name = self.identifier_name();
        let comma = ",";
        let attr_list = self.attr_list();
        format!("{}{}{}", identifier_name, comma, attr_list)
    }

    fn last_attr(&self) -> String {
        let identifier_name = self.identifier_name();
        format!("{}", identifier_name)
    }

    fn agg_statement(&self) -> Vec<String> {
        let mut agg_statement = Vec::new();
        let attr_list = self.attr_list();
        let semicolon = ";";
        let empty_statement = self.empty_statement();
        let agg_statement_result = self.agg_statement_result(attr_list, semicolon, empty_statement);
        for agg in agg_statement_result {
            agg_statement.push(agg);
        }
        agg_statement
    }
