

pub type BRANEName = &'static str;
pub const BRANE_DEFAULT: BRANEName = "default";
pub const BRANE_LOCK: BRANEName = "lock";
pub const BRANE_WRITE: BRANEName = "write";
pub const BRANE_violetabft: BRANEName = "violetabft";
pub const BRANE_VER_DEFAULT: BRANEName = "ver_default";
// BRANEs that should be very large generally.
pub const LARGE_BRANES: &[BRANEName] = &[BRANE_DEFAULT, BRANE_LOCK, BRANE_WRITE];
pub const ALL_BRANES: &[BRANEName] = &[BRANE_DEFAULT, BRANE_LOCK, BRANE_WRITE, BRANE_violetabft];
pub const DATA_BRANES: &[BRANEName] = &[BRANE_DEFAULT, BRANE_LOCK, BRANE_WRITE];

pub fn name_to_brane(name: &str) -> Option<BRANEName> {
    if name.is_empty() {
        return Some(BRANE_DEFAULT);
    }
    for c in ALL_BRANES {
        if name == *c {
            return Some(c);
        }
    }

    None
}
