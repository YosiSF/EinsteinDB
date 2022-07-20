## README for BEROLINASQL
# BerolinaSQL is a SQL engine for the EinsteinDB database.

Berolina is a shim implementation of a rust-peg context-ambient datascript-like interface to serialize and deserialize sql queries in the form of AllegroCL.

    
    []: # Language: rust
    []: # Path: berolinasql/src/main.rs
    ```rust
    use berolinasql::*;
    use std::io::{stdin, stdout, Write};

use rusty-peg::*;

    fn main() {
        let mut input = String::new();
        stdout().write(b">>> ").unwrap();
        stdin().read_line(&mut input).unwrap();
        let mut parser = Parser::new(input.trim());
        let result = parser.parse();
        println!("{:?}", result);
    }
    ```
    []: # Language: rust
    []: # Path: berolinasql/src/parser.rs
    ```


