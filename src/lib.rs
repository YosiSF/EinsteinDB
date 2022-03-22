
#![crate_type = "lib"]
#![recursion_limit = "400"]


mod query_builder;

pub extern crate einsten_ml;

#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate more_asserts;



pub fn einsteindb_version_info(build_time: Option<&str>) -> String {
    let mut version = String::from("Einsteindb ");
    version.push_str(env!("CARGO_PKG_VERSION"));
    if let Some(build_time) = build_time {
        version.push_str(format!(" (build {})", build_time).as_str());
    }
    version
}