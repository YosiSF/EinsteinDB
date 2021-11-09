


pub const BRANE_DEFAULT: BRANEName = "default";
pub const BRANE_LOCK: BRANEName = "lock";
pub const BRANE_WRITE: BRANEName = "write";
pub const BRANE_VIOLETABFT: BRANEName = "violetabft";
pub const BRANE_VER_DEFAULT: BRANEName = "ver_default";
// BRANEs that should be very large generally.
pub const LARGE_BRANES: &[BRANEName] = &[BRANE_DEFAULT, BRANE_LOCK, BRANE_WRITE];
pub const ALL_BRANES: &[BRANEName] = &[BRANE_DEFAULT, BRANE_LOCK, BRANE_WRITE, BRANE_VIOLETABFT];
pub const DATA_BRANES: &[BRANEName] = &[BRANE_DEFAULT, BRANE_LOCK, BRANE_WRITE];

/*
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

 */

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum BRANEName {
    Default,
    Lock,
    Write,
    Violetabft,
    VerDefault,
}

//write implementation
//impl for BRANEName {}

//the struct
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum BRANEType {
    Default,
    NeverFreeze,
}

impl for BRANEType{}!

#[derive(Default)]
pub struct BRANEParams {
    pub name: BRANEName,
    pub size: u64,
}

impl BRANEParams {
    pub fn new(name: BRANEName, size: u64) -> BRANEParams {
        BRANEParams {
            name: name,
            size: size,
        }
    }

    pub fn get_name(&self) -> &BRANEName {
        &self.name
    }
}

pub struct BRANES {
    pub branes: Vec<BRANEParams>,
}

impl Default for BRANES {
    fn default() -> BRANES {
        BRANES {
            branes: vec![
                BRANEParams::new(BRANE_DEFAULT, 0),
                BRANEParams::new(BRANE_LOCK, 0),
                BRANEParams::new(BRANE_WRITE, 0),
                BRANEParams::new(BRANE_VER_DEFAULT, 0),
                BRANEParams::new(BRANE_VIOLETABFT, 0),
            ],
        }
    }
}

impl BRANES {
    pub fn get_brane_params(&self, brane_name: BRANEName) -> Option<&BRANEParams> {
        self.branes.iter().find(|t| t.name == brane_name)
    }

    pub fn get_brane_params_mut(&mut self, brane_name: BRANEName) -> Option<&mut BRANEParams> {
        self.branes.iter_mut().find(|t| t.name == brane_name)
    }

    pub fn get_brane_name(&self, index: usize) -> Option<&BRANEName> {
        self.branes.get(index).map(|t| &t.name)
    }

    pub fn get_brane_name_mut(&mut self, index: usize) -> Option<&mut BRANEName> {
        self.branes.get_mut(index).map(|t| &mut t.name)
    }

    pub fn get_brane_size(&self, index: usize) -> Option<&u64> {
        self.branes.get(index).map(|t| &t.size)
    }

    pub fn get_brane_size_mut(&mut self, index: usize) -> Option<&mut u64> {
        self.branes.get_mut(index).map(|t| &mut t.size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name_to_brane() {
        assert_eq!(name_to_brane(""), Some(BRANE_DEFAULT));
        assert_eq!(name_to_brane("default"), Some(BRANE_DEFAULT));
        assert_eq!(name_to_brane("lock"), Some(BRANE_LOCK));
        assert_eq!(name_to_brane("write"), Some(BRANE_WRITE));
        assert_eq!(name_to_brane("violetabft"), Some(BRANE_VIOLETABFT));
        assert_eq!(name_to_brane("ver_default"), Some(BRANE_VER_DEFAULT));
        assert_eq!(name_to_brane("VerDefault"), Some(BRANE_VER_DEFAULT));
        assert_eq!(name_to_brane("violetabft"), Some(BRANE_VIOLETABFT));
        assert_eq!(name_to_brane("some_brane_name"), None);
    }

    #[test]
    fn test_name_to_brane_invalid() {
        assert_eq!(name_to_brane(""), None);
        assert_eq!(name_to_brane("default"), None);
        assert_eq!(name_to_brane("lock"), None);
        assert_eq!(name_to_brane("write"), None);
        assert_eq!(name_to_brane("violetab
}