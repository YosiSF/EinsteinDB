//ast for pushdown automata from foundation db key-value to gremlin tinkerpop
//einsteinml is a meta language inspired by allegrocl code generation
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
pub type Soliton = String;
pub type SolitonType = String;
pub type SolitonValue = String;
pub type SolitonValueType = String;
pub type TypeId = String;
pub type TypeName = String;
pub fn type_name(name: &str) -> TypeName {
    format!("{}", name)
}
pub type TypeNameList = Vec<TypeName>;
pub type TypeList = Vec<Type>;
pub type TypeIdList = Vec<TypeId>;
pub type TypeIdListList = Vec<TypeIdList>;


//! # AST
//!
//! The AST is the core of the compiler. It is a tree of nodes that represents the
//! program. Each node has a type and a list of children. The type of a node is
//! either a primitive type (e.g. `int`) or a user-defined type (e.g. `Foo`).
//!     let foo = 5;
//!    let bar = true;
//!   let baz = "test";
//!

///!cl-user(2): (require :prolog)
// t
// cl-user(3): (use-package :prolog)
// t
// cl-user(4): (<-- (rev-member ?item (? . ?rest))
//               (rev-member ?item ?rest))
// rev-member
// cl-user(5): (<- (rev-member ?item (?item . ?)))
// rev-member
// cl-user(6): (leash rev-member 2)
// t
// cl-user(7): (?- (rev-member ?animal (dog cat fish)))
// [1] Entering rev-member/2 {Unbound 1000fe7cb1} (dog cat fish)
//   [2] Entering rev-member/2 {Unbound 1000fe7cb1} (cat fish)
//     [3] Entering rev-member/2 {Unbound 1000fe7cb1} (fish)
//       [4] Entering rev-member/2 {Unbound 1000fe7cb1} ()
//       [4] Failed rev-member/2
//     [3] Succeeded rev-member/2 fish (fish)
//   [2] Succeeded rev-member/2 fish (cat fish)
// [1] Succeeded rev-member/2 fish (dog cat fish) ?animal="fish" <ENTER>
// [1] Backtracking into rev-member/2
//  [2] Backtracking into rev-member/2
//   [3] Backtracking into rev-member/2
//   [3] Failed rev-member/2
//  [2] Succeeded rev-member/2 cat (cat fish)
// [1] Succeeded rev-member/2 cat (dog cat fish)
// ?animal = cat <ENTER>
// [1] Backtracking into rev-member/2
//  [2] Backtracking into rev-member/2
//  [2] Failed rev-member/2
// [1] Succeeded rev-member/2 dog (dog cat fish)
// ?animal = dog <ENTER>
// [1] Backtracking into rev-member/2
// [1] Failed rev-member/2
// No.
// cl-user(8):

use ::EinsteinDB;


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LispOperators {
    /// `(=)`
    /// `(= a b)`
    /// `(= a b c)`
    /// `(= a b c d)`

    /// `(<)`
    /// `(< a b)`
    /// `(< a b c)`
    /// `(< a b c d)`
    /// `(< a b c d e)`
    /// `(< a b c d e f)`,

    Equal,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    NotEqual,
    /// `(+)`
    /// `(+ a b)`
    /// `(+ a b c)`
    /// `(+ a b c d)`
    /// `(+ a b c d e)`

    Add,
    /// `(-)`
    /// `(- a b)`
    /// `(- a b c)`

    Subtract,


}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LispTypes {
    /// `int`
    Int,
    /// `bool`
    Bool,
    /// `string`
    String,
    /// `float`
    Float,
    /// `list`
    List,
    /// `map`
    Map,
    /// `set`
    Set,
    /// `vector`
    Vector,
    /// `tuple`
    Tuple,
    /// `function`
    Function,
    /// `symbol`
    Symbol,
    /// `nil`
    Nil,
    /// `cons`
    Cons,
    /// `atom`
    Atom,
    /// `quote`
    Quote,
    /// `quasiquote`
    Quasiquote,
    /// `unquote`
    Unquote,
    /// `unquote-splicing`
    UnquoteSplicing,
    /// `macro`
    Macro,
    /// `user`
    User,
    /// `einstein-db`
    EinsteinDB,
    /// `einstein-ml`
    EinsteinML,
    /// `einstein-db-key-value`
    EinsteinDBKeyValue,
    /// `einstein-db-gremlin`
    EinsteinDBGremlin,
    /// `einstein-db-gremlin-tinkerpop`
    EinsteinDBGremlinTinkerpop,
    /// `einstein-db-gremlin-tinkerpop-graph`
    EinsteinDBGremlinTinkerpopGraph,
    /// `einstein-db-gremlin-tinkerpop-graph-vertex`
    EinsteinDBGremlinTinkerpopGraphVertex,
    /// `einstein-db-gremlin-tinkerpop-graph-edge`
    EinsteinDBGremlinTinkerpopGraphEdge,
    /// `einstein-db-gremlin-tinkerpop-graph-vertex-property`
    EinsteinDBGremlinTinkerpopGraphVertexProperty,
    /// `einstein-db-gremlin-tinkerpop-graph-edge-property`
    EinsteinDBGremlinTinkerpopGraphEdgeProperty,
    /// `einstein-db-gremlin-tinker
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LispType {
    pub name: String,
    /// The name of the type.
    pub type_name: String,

    /// The type of the type.
    /// This is either a primitive type or a user-defined type.
    /// For primitive types, the type name is the same as the name.
    /// For user-defined types, the type name is the name of the type.
    /// For example, the type name of `int` is `int`.
    /// The type name of `Foo` is `Foo`.
    /// The type name of `(Foo Bar)` is `(Foo Bar)`.

    pub type_type: LispTypes,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LispValue {
    pub value: String,
    pub type_: LispType,
}



#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LispExpression {
    pub operator: LispOperators,
    pub operands: Vec<LispExpression>,
}



//!cl-user(2): (require :prolog)
//! cl-user(3): (use-package :prolog)
//! cl-user(4): (<-- (rev-member ?item (? . ?rest))
//!              (rev-member ?item ?rest))
//! rev-member
//! cl-user(5): (<- (rev-member ?item (?item . ?)))
//! rev-member
//! cl-user(6): (leash rev-member 2)
//! cl-user(7): (?- (rev-member ?animal (dog cat fish)))
//! [1] Entering rev-member/2 {Unbound 1000fe7cb1} (dog cat fish)
//!  [2] Entering rev-member/2 {Unbound 1000fe7cb1} (cat fish)
//!





#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TypeKind< T > {
    /// `int`
    /// `bool`
    /// `string`
    /// `float`
    /// `list`
    /// `map`
    /// `set`
    /// `vector`
    Int,
    Bool,
    String,
    Float,
    List,
    Map,
    Set,
    Vector,
    Tuple,
    Function,
    Symbol,
    Nil,
    Cons,
    Atom,
    Quote,
    Quasiquote,
    Unquote,
    UnquoteSplicing,
    Macro,
    Primitive(T),
    User(TypeName),
}

async fn type_kind< T >(kind: &str) -> TypeKind< T > {
    match kind {
        "int" => TypeKind::Int,
        "bool" => TypeKind::Bool,
        "string" => TypeKind::String,
        "float" => TypeKind::Float,
        "list" => TypeKind::List,
        "map" => TypeKind::Map,
        "set" => TypeKind::Set,
        "vector" => TypeKind::Vector,
        "tuple" => TypeKind::Tuple,
        "function" => TypeKind::Function,
        "symbol" => TypeKind::Symbol,
        "nil" => TypeKind::Nil,
        "cons" => TypeKind::Cons,
        "atom" => TypeKind::Atom,
        "quote" => TypeKind::Quote,
        "quasiquote" => TypeKind::Quasiquote,
        "unquote" => TypeKind::Unquote,
        "unquote-splicing" => TypeKind::UnquoteSplicing,
        "macro" => TypeKind::Macro,
        _ => TypeKind::User(type_name(kind)),
    }
    match kind {
        "primitive" => TypeKind::Primitive(T::default()),
        "user" => TypeKind::User(TypeName::default()),
        _ => panic!("unknown type kind: {}", kind),
    }
}




fn type_kind_to_str< T >(kind: &TypeKind< T >) -> &str {
    match kind {
        TypeKind::Int => "int",
        TypeKind::Bool => "bool",
        TypeKind::String => "string",
        TypeKind::Float => "float",
        TypeKind::List => "list",
        TypeKind::Map => "map",
        TypeKind::Set => "set",
        TypeKind::Vector => "vector",
        TypeKind::Tuple => "tuple",
        TypeKind::Function => "function",
        TypeKind::Symbol => "symbol",
        TypeKind::Nil => "nil",
        TypeKind::Cons => "cons",
        TypeKind::Atom => "atom",
        TypeKind::Quote => "quote",
        TypeKind::Quasiquote => "quasiquote",
        TypeKind::Unquote => "unquote",
        TypeKind::UnquoteSplicing => "unquote-splicing",
        TypeKind::Macro => "macro",
        TypeKind::Primitive(_) => "primitive",
        TypeKind::User(name) => name.as_str(),
    }
    match kind {

        TypeKind::Primitive(_) => "primitive",
        TypeKind::User(_) => "user",
        /// TypeKind::Int => "int",
        /// TypeKind::Bool => "bool",
        TypeKind::String => "string",
        /// TypeKind::Float => "float",
        TypeKind::List => "list",
        TypeKind::Map => "map",
        TypeKind::Set => "set",
        TypeKind::Vector => "vector",
        TypeKind::Tuple => "tuple",
        TypeKind::Function => "function",
        TypeKind::Symbol => "symbol",
        TypeKind::Nil => "nil",
        TypeKind::Cons => "cons",
        TypeKind::Atom => "atom",
        TypeKind::Quote => "quote",
        TypeKind::Quasiquote => "quasiquote",

    }
}


fn type_kind_to_string< T >(kind: &TypeKind< T >) -> String {
    type_kind_to_str(kind).to_string()
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(dead_code)]
#[allow(unused_variables)]
#[allow(unused_assignments)]
pub struct Type< T > {
    pub kind: TypeKind<T>,
    pub name: TypeName,
    pub id: TypeId,
    pub params: TypeList,
    pub ret: TypeList,
    pub generics: TypeList,
    pub generics_id: TypeIdList,
}


/// `Type` is a type alias for `Rc<RefCell<Type>>`
///
///
///
/// CausetID is a type alias for `String`
/// use `String` as `CausetID` is a `String`
/// use `Rc<RefCell<Type>>` as `Type` is a `Rc<RefCell<Type>>`



pub trait Typeable< T > {
    fn type_kind(&self) -> TypeKind< T >;
    fn type_name(&self) -> TypeName;
    fn type_id(&self) -> TypeId;
    fn type_params(&self) -> TypeList;
    fn type_ret(&self) -> TypeList;
    fn type_generics(&self) -> TypeList;
    fn type_generics_id(&self) -> TypeIdList;
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
pub enum Statement {
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