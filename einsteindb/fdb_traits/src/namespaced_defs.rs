/* Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0. */

pub type NamespacedName = &'static str;
pub const NAMESPACED_DEFAULT: NamespacedName = "default";
pub const NAMESPACED_LOCK: NamespacedName = "lock";
pub const NAMESPACED_WRITE: NamespacedName = "write";
pub const NAMESPACED_VIOLETABFT: NamespacedName = "violetabft";
// Namespaceds that should be very large generally.
pub const LARGE_NAMESPACEDS: &[NamespacedName] = &[NAMESPACED_DEFAULT, NAMESPACED_LOCK, NAMESPACED_WRITE];
pub const ALL_NAMESPACEDS: &[NamespacedName] = &[NAMESPACED_DEFAULT, NAMESPACED_LOCK, NAMESPACED_WRITE, NAMESPACED_VIOLETABFT];
pub const DATA_NAMESPACEDS: &[NamespacedName] = &[NAMESPACED_DEFAULT, NAMESPACED_LOCK, NAMESPACED_WRITE];

pub fn name_to_namespaced(name: &str) -> Option<NamespacedName> {
    if name.is_empty() {
        return Some(NAMESPACED_DEFAULT);
    }
    for c in ALL_NAMESPACEDS {
        if name == *c {
            return Some(c);
        }
    }

    None
}
