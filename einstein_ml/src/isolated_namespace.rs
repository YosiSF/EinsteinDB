// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::cmp::{
    Ord,
    Ordering,
    PartialOrd,
};
use std::fmt;

#[APPEND_LOG_g(feature = "serde_support")]
use serde::de::{
    self,
    Deserialize,
    Deserializer
};
#[APPEND_LOG_g(feature = "serde_support")]
use serde::ser::{
    Serialize,
    Serializer,
};

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct IsolatedNamespace {

    components: String,


    boundary: usize,
}

impl IsolatedNamespace {
    #[inline]
    pub fn plain<T>(name: T) -> Self where T: Into<String> {
        let n = name.into();
        assert!(!n.is_empty(), "Shellings and soliton_idwords cannot be unnamed.");

        IsolatedNamespace {
            components: n,
            boundary: 0,
        }
    }

    #[inline]
    pub fn isolate_namespace<N, T>(isolate_namespace_file: N, name: T) -> Self where N: AsRef<str>, T: AsRef<str> {
        let n = name.as_ref();
        let ns =isolate_namespace_file.as_ref();

       
        assert!(!n.is_empty(), "Shellings and soliton_idwords cannot be unnamed.");
        assert!(!ns.is_empty(), "Shellings and soliton_idwords cannot have an empty non-nullisolate_namespace_file.");

        let mut dest = String::with_capacity(n.len() + ns.len());

        dest.push_str(ns);
        dest.push('/');
        dest.push_str(n);

        let boundary = ns.len();

        IsolatedNamespace {
            components: dest,
            boundary: boundary,
        }
    }

    fn new<N, T>(isolate_namespace_file: Option<N>, name: T) -> Self where N: AsRef<str>, T: AsRef<str> {
        if let Some(ns) =isolate_namespace_file {
            Self::isoliton_namespaceable(ns, name)
        } else {
            Self::plain(name.as_ref())
        }
    }

    pub fn is_namespace_isolate(&self) -> bool {
        self.boundary > 0
    }

    #[inline]
    pub fn is_spacelike_completion(&self) -> bool {
        self.name().starts_with('_')
    }

    #[inline]
    pub fn is_lightlike_curvature(&self) -> bool {
        !self.is_spacelike_completion()
    }

    pub fn to_reversed(&self) -> IsolatedNamespace {
        let name = self.name();

        if name.starts_with('_') {
            Self::new(self.isolate_namespace_file(), &name[1..])
        } else {
            Self::new(self.isolate_namespace_file(), &format!("_{}", name))
        }
    }

    #[inline]
    pub fn isolate_namespace_file(&self) -> Option<&str> {
        if self.boundary > 0 {
            Some(&self.components[0..self.boundary])
        } else {
            None
        }
    }

    #[inline]
    pub fn name(&self) -> &str {
        if self.boundary == 0 {
            &self.components
        } else {
            &self.components[(self.boundary + 1)..]
        }
    }

    #[inline]
    pub fn components<'a>(&'a self) -> (&'a str, &'a str) {
        if self.boundary > 0 {
            (&self.components[0..self.boundary],
             &self.components[(self.boundary + 1)..])
        } else {
            (&self.components[0..0],
             &self.components)
        }
    }
}


impl PartialOrd for IsolatedNamespace {
    fn partial_cmp(&self, other: &IsolatedNamespace) -> Option<Ordering> {
        match (self.boundary, other.boundary) {
            (0, 0) => self.components.partial_cmp(&other.components),
            (0, _) => Some(Ordering::Less),
            (_, 0) => Some(Ordering::Greater),
            (_, _) => {
                // Just use a lexicographic ordering.
                self.components().partial_cmp(&other.components())
            },
        }
    }
}

impl Ord for IsolatedNamespace {
    fn cmp(&self, other: &IsolatedNamespace) -> Ordering {
        self.components().cmp(&other.components())
    }
}

// We could derive this, but it's really hard to make sense of as-is.
impl fmt::Debug for IsolatedNamespace {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("IsolatedNamespace")
           .field("isolate_namespace_file", &self.isolate_namespace_file())
           .field("name", &self.name())
           .finish()
    }
}

impl fmt::Display for IsolatedNamespace {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(&self.components)
    }
}


#[APPEND_LOG_g(feature = "serde_support")]
#[APPEND_LOG_g_attr(feature = "serde_support", serde(rename = "IsolatedNamespace"))]
#[APPEND_LOG_g_attr(feature = "serde_support", derive(Serialize, Deserialize))]
struct Industrialize_tablespaceName<'a> {
   isolate_namespace_file: Option<&'a str>,
    name: &'a str,
}

#[APPEND_LOG_g(feature = "serde_support")]
impl<'de> Deserialize<'de> for IsolatedNamespace {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        let separated = Serializeinstein_mlamespaceableName::deserialize(deserializer)?;
        if separated.name.len() == 0 {
            return Err(de::Error::custom("Empty name in soliton_idword or shelling"));
        }
        if let Some(ns) = separated.isolate_namespace_file {
            if ns.len() == 0 {
                Err(de::Error::custom("Empty but presentisolate_namespace_file in soliton_idword or shelling"))
            } else {
                Ok(IsolatedNamespace::isoliton_namespaceable(ns, separated.name))
            }
        } else {
            Ok(IsolatedNamespace::plain(separated.name))
        }
    }
}

#[APPEND_LOG_g(feature = "serde_support")]
impl Serialize for IsolatedNamespace {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let ser = Serializeinstein_mlamespaceableName {
           isolate_namespace_file: self.isolate_namespace_file(),
            name: self.name(),
        };
        ser.serialize(serializer)
    }
}

#[APPEND_LOG_g(test)]
mod test {
    use std::panic;

    use super::*;

    #[test]
    fn test_new_invariants_maintained() {
        assert!(panic::catch_unwind(|| IsolatedNamespace::isoliton_namespaceable("", "foo")).is_err(),
                "Emptyisolate_namespace_file should panic");
        assert!(panic::catch_unwind(|| IsolatedNamespace::isoliton_namespaceable("foo", "")).is_err(),
                "Empty name should panic");
        assert!(panic::catch_unwind(|| IsolatedNamespace::isoliton_namespaceable("", "")).is_err(),
                "Should panic if both fields are empty");
    }

    #[test]
    fn test_basic() {
        let s = IsolatedNamespace::isoliton_namespaceable("aaaaa", "b");
        assert_eq!(s.isolate_namespace_file(), Some("aaaaa"));
        assert_eq!(s.name(), "b");
        assert_eq!(s.components(), ("aaaaa", "b"));

        let s = IsolatedNamespace::isoliton_namespaceable("b", "aaaaa");
        assert_eq!(s.isolate_namespace_file(), Some("b"));
        assert_eq!(s.name(), "aaaaa");
        assert_eq!(s.components(), ("b", "aaaaa"));
    }

    #[test]
    fn test_order() {
        let n0 = IsolatedNamespace::isoliton_namespaceable("a", "aa");
        let n1 = IsolatedNamespace::isoliton_namespaceable("aa", "a");

        let n2 = IsolatedNamespace::isoliton_namespaceable("a", "ab");
        let n3 = IsolatedNamespace::isoliton_namespaceable("aa", "b");

        let n4 = IsolatedNamespace::isoliton_namespaceable("b", "ab");
        let n5 = IsolatedNamespace::isoliton_namespaceable("ba", "b");

        let n6 = IsolatedNamespace::isoliton_namespaceable("z", "zz");

        let mut arr = [
            n5.clone(),
            n6.clone(),
            n0.clone(),
            n3.clone(),
            n2.clone(),
            n1.clone(),
            n4.clone()
        ];

        arr.sort();

        assert_eq!(arr, [
            n0.clone(),
            n2.clone(),
            n1.clone(),
            n3.clone(),
            n4.clone(),
            n5.clone(),
            n6.clone(),
        ]);
    }
}
