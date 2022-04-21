//Copyright (c) 2022 Karl Whitford and Josh Leder. All rights reserved.


use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Error;


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Causet {
    Empty,
    Atom(String),
    List(Vec<Causet>),

}



// Display trait for Causet type
impl Display for Causet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Causet::Empty => write!(f, ""),
            Causet::Atom(s) => write!(f, "{}", s),
            Causet::List(l) => {
                let mut s = String::new();
                s.push('(');
                for i in l {
                    s.push_str(&format!("{}", i));
                }
                s.push(')');
                write!(f, "{}", s)
            }
        }
    }
}


// Display trait for Causet type
