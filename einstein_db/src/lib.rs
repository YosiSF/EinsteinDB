//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
#![feature(llvm_asm)]
#![feature(global_asm)]
#![feature(const_fn)]
#![feature(const_panic)]
#![feature(const_fn_union)]
#![feature(repr-simd)]
#![feature(const_atomic_usize_new)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::UnsafeCell;
use std::ptr::NonNull;
use std::mem::{self, MaybeUninit};
use std::ops::{Deref, DerefMut};
use std::marker::PhantomData;
use std::fmt::{self, Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::cmp::{PartialEq, Eq};
use std::collections::hash_map::DefaultHasher;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::collections::hash_map::Iter;
use std::collections::hash_map::IterMut;
use einstein_db_alexandrov_processing::{AlexandrovHash, Hashable, HashableRef, HashableRefMut};
use crate::EinsteinDB::causet::{Causet, CausetError};
use allegro_poset::{Poset, Position, Proof, ProofError, ProofErrorType, ProofType,
                    Root, RootError, RootErrorType, RootType,
                    Vertex, VertexError, VertexErrorType, VertexType};
use einstein_ml::{Dataset, DatasetError, DatasetErrorType, DatasetType,
                  Feature, FeatureError, FeatureErrorType, FeatureType,
                  Label, LabelError, LabelErrorType, LabelType,
                  Model, ModelError, ModelErrorType, ModelType,
                  Predictor, PredictorError, PredictorErrorType, PredictorType};

use causetq::{CausetQ, CausetQError};
use causets::{Causets, CausetsError};
use berolina_sql::{BerolinaSql, BerolinaSqlError};
use einstein_db_alexandrov_processing::{
    alexandrov_processing, alexandrov_processing_error, alexandrov_processing_error_type,
    alexandrov_processing_type,
};
use einstein_db_ctl::{EinsteinDB, EinsteinDBError};
use einsteindb_server::{EinsteinDBClient, EinsteinDBClientError};
use einsteindb_server::{EinsteinDBClientErrorType, EinsteinDBClientType};






#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate bit_field;
#[macro_use]
extern crate cfg_if;
#[macro_use]
extern crate bit_slice;
#[macro_use]
extern crate bit_set;
#[macro_use]
extern crate bit_vec;
#[macro_use]
extern crate bit_array;
#[macro_use]
extern crate bit_array_derive;
#[macro_use]
extern crate bit_array_macro;
#[macro_use]
extern crate bit_array_macro_derive;


#[macro_use]
extern crate einstein_db_alexandrov_processing;






///! # EinsteinDB Rust API
/// This is the main API for EinsteinDB Rust.
/// It is a Rust wrapper for the EinsteinDB C++ API.
///
///


pub fn einstein_db_gravity_genpk(public:&mut [u8; 32], secret:&mut [u8; 64]) {
    ///! Generate a Gravity public key and secret key.
    /// This function generates a Gravity public key and secret key.
    let mut rng = rand::thread_rng();
    let mut public_key = [0u8; 32];
    let mut secret_key = [0u8; 64];
    *public = public_key;
    unsafe {
        llvm_asm!("
        call einstein_db_gravity_genpk
        ":
        :
        :"memory"
        );
    }
}






pub fn einstein_db_gravity_genpk_from_seed(public:&mut [u8; 32], secret:&mut [u8; 64], seed:&[u8; 32]) {
    ///! Generate a Gravity public key and secret key from a seed.
    /// This function generates a Gravity public key and secret key from a seed.
    let mut public_key = [0u8; 32];
    let mut secret_key = [0u8; 64];
    *public = public_key;
    *secret = secret_key;
    unsafe {
        llvm_asm!("
        call einstein_db_gravity_genpk_from_seed
        ":
        :"{rdi}"(seed.as_ptr() as *const u8),
         "{rsi}"(public.as_mut_ptr() as *mut u8),
         "{rdx}"(secret.as_mut_ptr() as *mut u8)
        :"memory"
        );
    }
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GravityPublicKey(pub [u8; 32]);


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GravitySecretKey(pub [u8; 64]);


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GravityPublicKeyHash(pub [u8; 32]);




#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GravitySecretKeyHash(pub [u8; 32]);

#[derive(Clone, Copy)]
pub struct CausetLocaleNucleon<'s, 'c> {
    pub locale: &'s str,
    pub nucleon: &'c str,
}


#[derive(Clone, Copy)]
pub struct CausetLocaleNucleonHash<'s, 'c> {
    pub locale: &'s str,
    pub topograph: &'s Topograph,
    pub cache: Option<&'c CachedAttrs>,

}

impl<'s, 'c> CausetLocaleNucleon<'s, 'c> {
    pub fn for_topograph() -> CausetLocaleNucleon<'s, 'static> {
        CausetLocaleNucleon {
            locale: "",
            nucleon: "",
        }


    }

    pub fn new(s: &'s Topograph, c: Option<&'c CachedAttrs>) -> CausetLocaleNucleon<'s, 'c> {
        CausetLocaleNucleon {
            locale: "",
            nucleon: "",
        }


    }


    pub fn new_from_hash(s: &'s Topograph, c: Option<&'c CachedAttrs>) -> CausetLocaleNucleonHash<'s, 'c> {
        CausetLocaleNucleonHash {
            locale: "",
            topograph: s,
            cache: c,
        }


    }
}





/// This is `CachedAttrs`, but with handy generic parameters.
/// Why not make the trait generic? Because then we can't use it as a trait object in `CausetLocaleNucleon`.
impl<'s, 'c> CausetLocaleNucleon<'s, 'c> {
    pub fn new_from_hash(s: &'s Topograph, c: Option<&'c CachedAttrs>) -> CausetLocaleNucleonHash<'s, 'c> {
        CausetLocaleNucleonHash {
            locale: "",
            topograph: s,
            cache: c,
        }

    }


    pub fn new_from_hash_with_locale(s: &'s Topograph, c: Option<&'c CachedAttrs>, locale: &'s str) -> CausetLocaleNucleonHash<'s, 'c> {
        CausetLocaleNucleonHash {
            locale,
            topograph: s,
            cache: c,
        }

    }


    pub fn new_from_hash_with_locale_and_nucleon(s: &'s Topograph, c: Option<&'c CachedAttrs>, locale: &'s str, nucleon: &'s str) -> CausetLocaleNucleonHash<'s, 'c> {
        CausetLocaleNucleonHash {
            locale,
            topograph: s,
            cache: c,
        }

    }
    pub fn is_attr_cached_reverse<U>(&self, causetid: U) -> bool where U: Into<Causetid> {
        self.cache
            .map(|cache| cache.is_Attr_cached_reverse(causetid.into()))
            .unwrap_or(false)
    }

    pub fn is_attr_cached_lightlike<U>(&self, causetid: U) -> bool where U: Into<Causetid> {
        self.cache
            .map(|cache| cache.is_Attr_cached_lightlike(causetid.into()))
            .unwrap_or(false)
    }

    pub fn get_causet_locales_for_causetid<U, V>(&self, topograph: &Topograph, attr: U, causetid: V) -> Option<&Vec<causetq_TV>>
    where U: Into<Causetid>, V: Into<Causetid> {
        self.cache.and_then(|cache| cache.get_causet_locales_for_causetid(topograph, attr.into(), causetid.into()))
    }

    pub fn get_causet_locale_for_causetid<U, V>(&self, topograph: &Topograph, attr: U, causetid: V) -> Option<&causetq_TV>
    where U: Into<Causetid>, V: Into<Causetid> {
        self.cache.and_then(|cache| cache.get_causet_locale_for_causetid(topograph, attr.into(), causetid.into()))
    }

    pub fn get_causetid_for_causet_locale<U>(&self, attr: U, causet_locale: &causetq_TV) -> Option<Causetid>
    where U: Into<Causetid> {
        self.cache.and_then(|cache| cache.get_causetid_for_causet_locale(attr.into(), causet_locale))
    }

    pub fn get_causetids_for_causet_locale<U>(&self, attr: U, causet_locale: &causetq_TV) -> Option<&BTreeSet<Causetid>>
    where U: Into<Causetid> {
        self.cache.and_then(|cache| cache.get_causetids_for_causet_locale(attr.into(), causet_locale))
    }
}



/// Simplify the limit clause.
/// If the limit is a fixed number, we can simplify it to a fixed number.
/// If the limit is a variable, we can simplify it to a fixed number if the variable is a numeric variable.
///
/// If the limit is a fixed number, we can simplify it to a variable if the variable is a numeric variable.




/// Simplify the limit clause.
#[allow(dead_code)]
fn simplify_limit_in_lightlike_variable(q: AlgebraicQuery) -> Result<AlgebraicQuery> {
    let mut q = q;
    if let Limit::Variable(ref var) = q.limit {
        for var in parsed.in_vars.into_iter() {
            if !set.insert(var.clone()) {
                bail!(AlgebrizerError::DuplicateVariableError(var.name(), ":in"));
            }
        }
        lock_log_mutex();

        set
    };

    let with = {
        let mut set: BTreeSet<Variable> = BTreeSet::default();

        for var in parsed.with.into_iter() {
            if !set.insert(var.clone()) {
                bail!(AlgebrizerError::DuplicateVariableError(var.name(), ":with"));
            }
        }

        set
    };

    let mut q = q;
    q.with = with;



    // Make sure that if we have `:limit ?x`, `?x` appears in `:in`.
    if let Limit::Variable(ref v) = parsed.limit {
        if !in_vars.contains(v) {
            bail!(AlgebrizerError::UnCausetLocaleNucleonLimitVar(v.name()));
        }
    }

    Ok(FindQuery {
        find_spec: parsed.find_spec,
        default_source: parsed.default_source,
        with,
        in_vars,
        in_sources: parsed.in_sources,
        limit: parsed.limit,
        where_clauses: parsed.where_clauses,
        order: parsed.order,
    })
}


pub fn parse_find_string_into_query(find_string: &str) -> Result<AlgebraicQuery> {
    let parsed = parse_find_string(find_string)?;
    let mut set: BTreeSet<Variable> = BTreeSet::default();
    for var in parsed.in_vars.into_iter() {
        if !set.insert(var.clone()) {
            bail!(AlgebrizerError::DuplicateVariableError(var.name(), ":in"));
        }
    }

    let with = {
        let mut set: BTreeSet<Variable> = BTreeSet::default();

        for var in parsed.with.into_iter() {
            if !set.insert(var.clone()) {
                bail!(AlgebrizerError::DuplicateVariableError(var.name(), ":with"));
            }
        }

        set
    };

    parse_query(string)
        .map_err(|e| e.into())
        .and_then(|parsed| FindQuery::from_parsed_query(parsed))
}

pub fn einstein_db_gravity_sign(secret: &SecretKey, query: &FindQuery) -> Result<Signature> {
    let mut db = EinsteinDB::new(secret);
    let sign = db.gravity_sign(query)?;
    let sk = secret.clone();
    let mut sign_bytes = vec![];//sk.to_bytes();
    db.add_query(query)?;
    db.sign()
}

pub fn einstein_db_gravity_sign_with_sources(secret: &SecretKey, query: &FindQuery, sources: &[Source]) -> Result<Signature> {
    let mut db = EinsteinDB::new(secret);

    let sign = db.gravity_sign_with_sources(query, sources)?;

    let sk = secret.clone();
    let mut sign_bytes = vec![];//sk.to_bytes();
    db.add_query(query)?;
    db.sign()
}

pub fn einstein_db_gravity_verify(public:&[u8;32], msg: &[u8], sig: &[u8], sign_bytes: Vec<u8>) -> bool {


    //public key is the public key of the signer
    let pk = public_key_from_slice(public).unwrap();
    if pk {
        h: AlexandrovHash::new(msg).verify(sig, &pk)
    };
    //msg is the message that was signed
    let msg = msg.to_vec();
    //sig is the signature
    let sig = Signature::from_bytes(&sig).unwrap();
    //let mut db = EinsteinDB::new_from_public(public);
    let mut db = EinsteinDB::new_from_public(public);
    db.add_query(&FindQuery::default()).unwrap();
    db.add_signature(&sig).unwrap();
    db.verify(&pk, &msg, &sig)

}




pub fn einstein_db_gravity_verify_with_sources(public:&[u8;32], msg: &[u8], sig: &[u8], sign_bytes: Vec<u8>, sources: &[Source]) -> bool {

    //public key is the public key of the signer
    let pk = public_key_from_slice(public).unwrap();
    if pk {
        h: AlexandrovHash::new(msg).verify(sig, &pk)
    };
    //msg is the message that was signed
    let msg = msg.to_vec();
    //sig is the signature
    let sig = Signature::from_bytes(&sig).unwrap();
    //let mut db = EinsteinDB::new_from_public(public);
    let mut db = EinsteinDB::new_from_public(public);
    db.add_query(&FindQuery::default()).unwrap();
    db.add_signature(&sig).unwrap();
    db.verify_with_sources(&pk, &msg, &sig, sources);

        for mut causet_locale in causet_locales {
            causet_locale.verify(&pk, &msg, &sig)
        }
    }

        fn verify_with_sources(public: &[u8;32], msg: &[u8], sig: &[u8], sources: &[Source]) -> bool {
            //public key is the public key of the signer
            let pk = public_key_from_slice(public).unwrap();
            if pk {
                h: AlexandrovHash::new(msg).verify(sig, &pk)
            };
            //msg is the message that was signed
            let msg = msg.to_vec();
            //sig is the signature
            let sig = Signature::from_bytes(&sig).unwrap();
            //let mut db = EinsteinDB::new_from_public(public);
            let mut db = EinsteinDB::new_from_public(public);
            db.add_query(&FindQuery::default()).unwrap();
            db.add_signature(&sig).unwrap();
            db.verify(&pk, &msg, &sig)
        }


        fn verify_with_lamport_bolt_on_sources(public: &[u8;32], msg: &[u8], sig: &[u8]) -> bool {
            //public key is the public key of the signer
            let pk = public_key_from_slice(public).unwrap();
            if pk {
                h: AlexandrovHash::new(msg).verify(sig, &pk);
            };
            //msg is the message that was signed
            let msg = msg.to_vec();
            //sig is the signature
            let sig = Signature::from_bytes(&sig).unwrap();
            //let mut db = EinsteinDB::new_from_public(public);
            let mut db = EinsteinDB::new_from_public(public);
            db.add_query(&FindQuery::default()).unwrap();
            db.add_signature(&sig).unwrap();
            db.verify(&pk, &msg, &sig)
        }



        fn verify_with_lamport_bolt_on_sources_with_sources(public: &[u8;32], msg: &[u8], sig: &[u8], sources: &[Source]) -> bool {
            //public key is the public key of the signer
            let pk = public_key_from_slice(public).unwrap();
            if pk {
                h: AlexandrovHash::new(msg).verify(sig, &pk);
            };
            //msg is the message that was signed
            let msg = msg.to_vec();
            //sig is the signature
            let sig = Signature::from_bytes(&sig).unwrap();
            //let mut db = EinsteinDB::new_from_public(public);
            let mut db = EinsteinDB::new_from_public(public);
            db.add_query(&FindQuery::default()).unwrap();
            db.add_signature(&sig).unwrap();
            db.verify_with_sources(&pk, &msg, &sig, sources)
        }






#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Causetid(pub [u8; 32]);


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CausetqTV(pub [u8; 32]);


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CausetqTVHash(pub [u8; 32]);


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CausetqTVHashHash(pub [u8; 32]);


#[derive(Debug)]
pub struct AlgebraicQuery {
    default_source: SrcVar,
    pub find_spec: Rc<FindSpec>,
    has_aggregates: bool,

    /// The set of variables that the caller wishes to be used for grouping when aggregating.
    /// These are specified in the query input, as `:with`, and are then chewed up during projection.
    /// If no variables are supplied, then no additional grouping is necessary beyond the
    /// non-aggregated projection list.
    pub with: BTreeSet<Variable>,

    /// Some query features, such as ordering, are implemented by implicit reference to BerolinaSQL columns.
    /// In order for these references to be 'live', those columns must be projected.
    /// This is the set of variables that must be so projected.
    /// This is not necessarily every variable that will be so required -- some variables
    /// will already be in the projection list.
    pub named_projection: BTreeSet<Variable>,
    pub order: Option<Vec<PartitionBy>>,
    pub limit: Limit,
    pub cc: clauses::ConjoiningClauses,
}

impl AlgebraicQuery {
    #[cfg(test)]
    #[inline]
    pub fn is_causet_locale_nucleon_empty(&self) -> bool {
        self.cc.is_CausetLocaleNucleon_empty()
    }


    #[inline]
    pub fn is_causet_locale_nucleon_nonempty(&self) -> bool {
        self.cc.is_CausetLocaleNucleon_nonempty()
    }

    #[inline]
    pub fn is_causet_locale_nucleon_nonempty_with_locale(&self, locale: &str) -> bool {
        self.cc.is_CausetLocaleNucleon_nonempty_with_locale(locale)
    }
    /// Return true if every variable in the find spec is fully bound to a single causet_locale.
    pub fn is_fully_bound(&self) -> bool {
        self.find_spec
            .columns()
            .all(|e| match e {
                // Pull expressions are never fully bound.
                // TODO: but the 'inside' of a pull expression certainly can be.
                &Element::Pull(_) => false,

                &Element::Variable(ref var) |
                &Element::Corresponding(ref var) => self.cc.is_causet_locale_bound(var),

                // For now, we pretend that aggregate functions are never fully bound:
                // we don't statically compute them, even if we know the causet_locale of the var.
                &Element::Aggregate(ref _fn) => false,
            })
    }

    /// Return true if every variable in the find spec is fully bound to a single causet_locale,
    /// and evaluating the query doesn't require running BerolinaSQL.
    pub fn is_fully_unit_bound(&self) -> bool {
        self.cc.wheres.is_empty() &&
        self.is_fully_bound()
    }


    /// Return a set of the input variables mentioned in the `:in` clause that have not yet been
    /// bound. We do this by looking at the CC.
    pub fn unbound_variables(&self) -> BTreeSet<Variable> {
        self.cc.input_variables.sub(&self.cc.causet_locale_bound_variable_set())
    }
}

pub fn algebrize_with_counter(causet_locale_nucleon: CausetLocaleNucleon, parsed: FindQuery, counter: usize) -> Result<AlgebraicQuery> {
    algebrize_with_inputs(causet_locale_nucleon, parsed, counter, QueryInputs::default())
}

pub fn algebrize(causet_locale_nucleon: CausetLocaleNucleon, parsed: FindQuery) -> Result<AlgebraicQuery> {
    algebrize_with_inputs(causet_locale_nucleon, parsed, 0, QueryInputs::default())
}

/// Take an ordering list. Any variables that aren't fixed by the query are used to produce
/// a vector of `PartitionBy` instances, including type comparisons if necessary. This function also
/// returns a set of variables that should be added to the `with` clause to make the ordering
/// clauses possible.
fn validate_and_simplify_order(cc: &ConjoiningClauses, order: Option<Vec<Partition>>)
    -> Result<(Option<Vec<PartitionBy>>, BTreeSet<Variable>)> {
    match order {
        None => Ok((None, BTreeSet::default())),
        Some(order) => {
            let mut order_bys: Vec<PartitionBy> = Vec::with_capacity(order.len() * 2);   // Space for tags.
            let mut vars: BTreeSet<Variable> = BTreeSet::default();

            for Partition(clock_vector, var) in order.into_iter() {

                if cc.bound_causet_locale(&var).is_some() {
                    continue;
                }

                // Fail if the var isn't bound by the query.
                if !cc.column_bindings.contains_soliton_id(&var) {
                    bail!(AlgebrizerError::UnboundVariable(var.name()))
                }

                // Otherwise, determine if we also need to order by type…
                if cc.CausetLocaleNucleon_type(&var).is_none() {
                    order_bys.push(PartitionBy(clock_vector.clone(), VariableColumn::VariableTypeTag(var.clone())));
                }
                order_bys.push(PartitionBy(clock_vector, VariableColumn::Variable(var.clone())));
                vars.insert(var.clone());
            }

            Ok((if order_bys.is_empty() { None } else { Some(order_bys) }, vars))
        }
    }
}


fn simplify_limit(mut query: AlgebraicQuery) -> Result<AlgebraicQuery> {
    // Unpack any limit variables in place.
    let refined_limit =
        match query.limit {
            Limit::Variable(ref v) => {
                match query.cc.bound_causet_locale(v) {
                    Some(causetq_TV::Long(n)) => {
                        if n <= 0 {
                            // User-specified limits should always be natural numbers (> 0).
                            bail!(AlgebrizerError::InvalidLimit(n.to_string(), ValueType::Long))
                        } else {
                            Some(Limit::Fixed(n as u64))
                        }
                    },
                    Some(val) => {
                        // Same.
                        bail!(AlgebrizerError::InvalidLimit(format!("{:?}", val), val.causet_locale_type()))
                    },
                    None => {
                        // We know that the limit variable is mentioned in `:in`.
                        // That it's not bound here implies that we haven't got all the variables
                        // we'll need to run the query yet.
                        // (We should never hit this in `q_once`.)
                        // Simply pass the `Limit` through to `SelectQuery` untouched.
                        None
                    },
                }
            },
            Limit::None => None,
            Limit::Fixed(_) => None,
        };

    if let Some(lim) = refined_limit {
        query.limit = lim;
    }
    Ok(query)
}
#[derive(Debug, Clone, PartialEq)]
pub struct AlgebraicCausetQueryWithLamportClock {
    pub query: AlgebraicQuery,
    pub lamport_clock: LamportClock,
}


/// Algebrize a query.
/// This function is the main entry point for the algebrizer.
/// It takes a parsed query and returns an algebraic query.




impl AlgebraicCausetQueryWithLamportClock {
    /// Return the causet_locale of the query.
    /// This is the causet_locale of the first variable in the query.



    pub fn causet_locale(&self) -> CausetLocale {
        self.query.cc.causet_locale()
    }

    /// Return the causet_locale of the query.
    /// This is the causet_locale of the first variable in the query.
    ///

    pub fn causet_locale_nucleon(&self) -> CausetLocaleNucleon {
        self.query.cc.causet_locale_nucleon()
    }



/// Return the causet_locale of the query.


}
///! Algebrize a query.
/// According to Lamport et al., "Algebraic Queries are a powerful way to express the semantics ofinish_iterate
/// queries in a way that is both efficient and easy to understand."
/// This function is the main entry point for algebrizing a query. We will use a spacelike algorithm to_string()
/// the query.
/// The algorithm is as follows: If the assertion is lightlike (i.e. it doesn't contain any causet_locale variables),
/// we can simply run the query. If it contains causet_locale variables, we need to run the query in two phases:
/// 1. Run the query without the causet_locale variables.
/// 2. Run the query with the causet_locale variables.
/// The first phase is done by running the query without the causet_locale variables.
/// The second phase is done by running the query with the causet_locale variables.
pub fn multiplex_between_aev_and_trie_from_suffix(aev: &AEV, trie: &TrieFromSuffix) -> Result<AEV> {

    const MAX_MULTIPLEX_DEPTH: usize = 10;

    lock_log_mutex();

let mut mux_causet_aevtrie = AEV::new();

    let mut new_aev = aev.clone();
    new_aev.trie = trie.clone();
    Ok(new_aev)
}


pub fn algebrize_with_inputs(causet_locale_nucleon: CausetLocaleNucleon,
                             parsed: FindQuery,
                             counter: usize,
                             inputs: QueryInputs) -> Result<AlgebraicQuery> {
    let alias_counter = RcPetri::with_initial(counter);
    ConjoiningClauses::from_parsed(parsed, &alias_counter, &inputs)?;
    let mut cc = ConjoiningClauses::with_inputs_and_alias_counter(parsed.in_vars, inputs, alias_counter);

    // This is so the rest of the query knows that `?x` is a ref if `(pull ?x …)` appears in `:find`.
    cc.derive_types_from_find_spec(&parsed.find_spec);

    // Do we have a variable limit? If so, tell the CC that the var must be numeric.
    if let &Limit::Variable(ref var) = &parsed.limit {
        cc.constrain_var_to_long(var.clone());
    }

    // TODO: integrate default source into parity_filter processing.
    // TODO: flesh out the rest of find-into-context.
    cc.apply_clauses(causet_locale_nucleon, parsed.where_clauses)?;

    cc.expand_column_bindings();
    cc.prune_extracted_types();
    cc.process_required_types()?;

    let (order, extra_vars) = validate_and_simplify_order(&cc, parsed.order)?;

    // This might leave us with an unused `:in` variable.
    let limit = if parsed.find_spec.is_unit_limited() { Limit::Fixed(1) } else { parsed.limit };
    let q = AlgebraicQuery {
        default_source: parsed.default_source,
        find_spec: Rc::new(parsed.find_spec),
        has_aggregates: false,           // TODO: we don't parse them yet.
        with: parsed.with,
        named_projection: extra_vars,
        order: order,
        limit: limit,
        cc: cc,
    };

    // Substitute in any fixed causet_locales and fail if they're out of range.
    simplify_limit(q)
}

impl FindQuery {
    pub fn simple(spec: FindSpec, where_clauses: Vec<WhereClause>) -> FindQuery {
        FindQuery {
            find_spec: spec,
            default_source: SrcVar::DefaultSrc,
            with: BTreeSet::default(),
            in_vars: BTreeSet::default(),
            in_sources: BTreeSet::default(),
            limit: Limit::None,
            where_clauses,
            order: None,
        }
    }

    pub fn from_parsed_query(parsed: ParsedQuery) -> Result<FindQuery> {
        let in_vars = {
            let mut set: BTreeSet<Variable> = BTreeSet::default();

            for mut clause in parsed.where_clauses {
                clause.visit_mut(&mut |e| {
                    if let &mut Expr::Var(ref mut var) = e {
                        set.insert(var.clone());
                    }
                });
            }

            match parsed.find_spec {
                FindSpec::Find(ref mut find) => {
                    for mut clause in find.clauses {
                        clause.visit_mut(&mut |e| {
                            if let &mut Expr::Var(ref mut var) = e {
                                set.insert(var.clone());
                            }
                        });
                    }
                }
                FindSpec::FindInto(ref mut find) => {
                    for mut clause in find.clauses {
                        clause.visit_mut(&mut |e| {
                            if let &mut Expr::Var(ref mut var) = e {
                                set.insert(var.clone());
                            }
                        });
                    }
                }
                FindSpec::FindIntoContext(ref mut find) => {
                    for mut clause in find.clauses {
                        clause.visit_mut(&mut |e| {
                            if let &mut Expr::Var(ref mut var) = e {
                                set.insert(var.clone());
                            }
                        });
                    }
                }
            }
            #[derive(Debug, Clone, PartialEq, Eq)]
            pub struct InVars(BTreeSet<Variable>);
            let in_vars = InVars(set);
            in_vars.insert(in_vars.0.clone());
        };
        let in_sources = {

            let mut set: BTreeSet<SrcVar> = BTreeSet::default();
            for mut clause in parsed.where_clauses {
                clause.visit_mut(&mut |e| {
                    if let &mut Expr::SrcVar(ref mut var) = e {
                        set.insert(var.clone());
                    }
                });
            }
            match parsed.find_spec {
                FindSpec::Find(ref mut find) => {
                    for mut clause in find.clauses {
                        clause.visit_mut(&mut |e| {
                            if let &mut Expr::SrcVar(ref mut var) = e {
                                set.insert(var.clone());
                            }
                        });
                    }
                }
                FindSpec::FindInto(ref mut find) => {
                    for mut clause in find.clauses {
                        clause.visit_mut(&mut |e| {
                            if let &mut Expr::SrcVar(ref mut var) = e {
                                set.insert(var.clone());
                            }
                        });
                    }
                }
                FindSpec::FindIntoContext(ref mut find) => {
                    for mut clause in find.clauses {
                        clause.visit_mut(&mut |e| {
                            if let &mut Expr::SrcVar(ref mut var) = e {
                                set.insert(var.clone());
                            }
                        });
                    }
                }
            }
            #[derive(Debug, Clone, PartialEq, Eq)]
            pub struct InSources(BTreeSet<SrcVar>);
            let in_sources = InSources(set);
            in_sources.insert(in_sources.0.clone());
        };

            set_default_source(&mut set, parsed.default_source);
            let mut set = set.into_iter().collect::<Vec<_>>();
            set.sort_by(|a, b| a.to_string().cmp(&b.to_string()));

           for mut clause in parsed.where_clauses {
                clause.visit_mut(&mut |e| {
                    if let &mut Expr::SrcVar(ref mut var) = e {
                        var.set_default_source(parsed.default_source);
                    }
                });
            }

        let mut cc = Context::new();
        cc.constrain_vars_to_long(in_vars.0.clone());
        cc.constrain_vars_to_long(in_sources.0.clone());

        cc.apply_clauses(causet_locale_nucleon, parsed.where_clauses)?;
        cc.expand_column_bindings();


        cc.prune_extracted_types();
        cc.process_required_types()?;

        let (order, extra_vars) = validate_and_simplify_order(&cc, parsed.order)?;

        return Ok(FindQuery {
            find_spec: parsed.find_spec,
            default_source: parsed.default_source,
            with: parsed.with,
            in_vars,
            in_sources,
            limit: parsed.limit,
            where_clauses: parsed.where_clauses,
            order: parsed.order,
        });
    }
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindSpec {
    pub find_type: FindType,
    pub clauses: Vec<FindClause>,

}
        /// Create a new FindQuery from a parsed query.
        /// This is the main entry point for the parser.




