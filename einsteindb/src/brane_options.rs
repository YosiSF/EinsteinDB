use crate::einsteindb::EinsteinMerkleEngine;
use einsteindb_promises::BRANENamesExt;

impl BRANENamesExt for EinsteinMerkleEngine {

    fn brane_names(&self) -> Vec<&str>{
    self.as_inner().brane_names()
    }
}