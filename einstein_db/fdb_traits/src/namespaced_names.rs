// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

pub trait NAMESPACEDNamesExt {
    fn namespaced_names(&self) -> Vec<&str>;
}
