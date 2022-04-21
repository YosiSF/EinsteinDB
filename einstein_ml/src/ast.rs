//ast for pushdown automata from foundation db key-value to gremlin tinkerpop
//einsteinml is a meta language inspired by allegrocl code generation


//! # AST
//!
//! The AST is the core of the compiler. It is a tree of nodes that represents the
//! program. Each node has a type and a list of children. The type of a node is
//! either a primitive type (e.g. `int`) or a user-defined type (e.g. `Foo`).
//!     let foo = 5;
//!    let bar = true;
//!   let baz = "test";
//!


use std::fmt;
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::LinkedList;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::LinkedList;


use std::rc::Rc;
use std::cell::RefCell;



pub type Solitonid = String;
pub type Type = String;
pub type TypeId = String;
pub type TypeName = String;

pub fn type_name(name: &str) -> TypeName {
    format!("{}", name)
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TypeKind {
    Primitive(Type),
    UserDefined(TypeName),
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TypeRef {
    Primitive(Type),
    UserDefined(TypeName),
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Copy)]
pub enum PrimitiveType {
    Int,
    Causetid,
    Solitonid,
    Float,
    AEVTrie,
    AEVTrieNode,
    Bool,
    String,
    Char,
    Unit,
    Option(Box<TypeRef>),
    List(Box<TypeRef>),
    Map(Box<TypeRef>, Box<TypeRef>),
    Set(Box<TypeRef>),
    Tuple(Vec<TypeRef>),
}





//! The AST is constructed by the parser and is then type checked by the type
//! checker.
//!
//! ## Nodes
//!

//dag nodes with line graph edges
//is a causet dag with poset coset and solitonid


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Node {
    //program
    Program(Vec<Statement>),
    //statement
    Statement(Statement),
    //expression
    Expression(Expression),
    //declaration
    Declaration(Declaration),
    //type
    Type(Type),
    //type declaration
    TypeDeclaration(TypeDeclaration),
    //type alias
    TypeAlias(TypeAlias),
    //type parameter

    //type parameter constraint list
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Node {
    // Statements
    Let(Solitonid, Box<Node>),
    LetRec(Solitonid, Vec<Solitonid>, Vec<Node>, Box<Node>),
    LetTuple(Vec<Solitonid>, Vec<Node>, Box<Node>),
    LetTupleRec(Vec<Solitonid>, Vec<Node>, Vec<Node>, Box<Node>),
    LetTupleStruct(Vec<Solitonid>, Vec<Node>, Box<Node>),

    // Expressions
    Literal(Literal),
    Solitonid(solitonid),
    Causetid(causetid),

    // Type
    Type(Type),
    TypeName(TypeName),
    TypeRef(TypeRef),
    TypeKind(TypeKind),
    TypeKinds(Vec<TypeKind>),
    TypeRefs(Vec<TypeRef>),

    // Patterns
    Pattern(Pattern),
    PatternName(Solitonid),

}







//! The AST is a tree of nodes. Each node represents a statement or expression.
//! The node type is either `Statement` or `Expression`.
//!     let foo = 5;
//!   let bar = true;
//! let baz = "test";
//!
//! ## Statements
//!
//! Statements are the main part of the AST. They represent actions that can be
//! performed on the program.

use std::collections::HashMap;
use std::collections::HashSet;
//gremlin tinkerpop
use gremlin::{Client, GraphTraversalSource, GremlinResult};
use gremlin::structure::{Vertex, Edge, Direction, VertexProperty};
use gremlin::process::traversal::strategy::vertex_program::VertexProgram;
use gremlin::process::traversal::step::util::EmptyStep;
use gremlin::process::traversal::step::map::MapStep;
use gremlin::process::traversal::step::filter::FilterStep;
use gremlin::process::traversal::step::sideEffect::SideEffectStep;
use gremlin::process::traversal::step::map::VertexStep;