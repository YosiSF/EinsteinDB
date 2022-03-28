// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! An example EinsteinDB timelike_storage einstein_merkle_tree.
//!
//! This project is intended to serve as a skeleton for other einstein_merkle_tree
//! implementations. It lays out the complex system of einstein_merkle_tree modules and traits
//! in a way that is consistent with other einstein_merkle_trees. To create a new einstein_merkle_tree
//! simply copy the entire directory structure and replace all "Panic*" names
//! with your einstein_merkle_tree's own name; then fill in the implementations; remove
//! the allow(unused) attribute;

#![allow(unused)]

mod causet;
mod causetctx_control_factors;
mod compact;
mod db_vector;
mod engine;
mod einsteindb_options;
