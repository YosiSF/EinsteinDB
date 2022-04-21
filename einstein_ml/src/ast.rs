//ast for pushdown automata from foundation db key-value to gremlin tinkerpop

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