use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{Result, VortexRdfError};

// VORTEX_RDF_CONFIGURABLE_NATIVE_INDEX_MODEL_V1
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NativeIndexSpec {
    SubjectRangesV1,
    PredicateRunsV1,
    PredicateExactRangesV2,
    PredicateObjectExactRangesV2,
    ObjectExactRangesV2,
}

impl NativeIndexSpec {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SubjectRangesV1 => "subject:ranges-v1",
            Self::PredicateRunsV1 => "predicate:runs-v1",
            Self::PredicateExactRangesV2 => "predicate:exact-ranges-v2",
            Self::PredicateObjectExactRangesV2 => "predicate-object:exact-ranges-v2",
            Self::ObjectExactRangesV2 => "object:exact-ranges-v2",
        }
    }
}

impl fmt::Display for NativeIndexSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for NativeIndexSpec {
    type Err = VortexRdfError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "subject:ranges-v1" => Ok(Self::SubjectRangesV1),
            "predicate:runs-v1" => Ok(Self::PredicateRunsV1),
            "predicate:exact-ranges-v2" => Ok(Self::PredicateExactRangesV2),
            "predicate-object:exact-ranges-v2" => Ok(Self::PredicateObjectExactRangesV2),
            "object:exact-ranges-v2" => Ok(Self::ObjectExactRangesV2),
            other => Err(VortexRdfError::InvalidOperation(format!(
                "unsupported native index specification {other:?}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NativeIndexProfile {
    None,
    /// Current subject-range plus experimental predicate-run artifact.
    Bootstrap,
    /// Proven v9-equivalent subject, predicate, predicate-object, and object family.
    #[default]
    Standard,
    /// Standard plus alternative experimental implementations.
    All,
}

impl NativeIndexProfile {
    pub fn specs(self) -> Vec<NativeIndexSpec> {
        match self {
            Self::None => Vec::new(),
            Self::Bootstrap => vec![
                NativeIndexSpec::SubjectRangesV1,
                NativeIndexSpec::PredicateRunsV1,
            ],
            Self::Standard => vec![
                NativeIndexSpec::SubjectRangesV1,
                NativeIndexSpec::PredicateExactRangesV2,
                NativeIndexSpec::PredicateObjectExactRangesV2,
                NativeIndexSpec::ObjectExactRangesV2,
            ],
            Self::All => vec![
                NativeIndexSpec::SubjectRangesV1,
                NativeIndexSpec::PredicateRunsV1,
                NativeIndexSpec::PredicateExactRangesV2,
                NativeIndexSpec::PredicateObjectExactRangesV2,
                NativeIndexSpec::ObjectExactRangesV2,
            ],
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativeIndexSelection {
    pub profile: NativeIndexProfile,
    /// Nonempty explicit specs override the profile.
    pub explicit: Vec<NativeIndexSpec>,
}

impl NativeIndexSelection {
    pub fn resolved(&self) -> Vec<NativeIndexSpec> {
        let mut specs = if self.explicit.is_empty() {
            self.profile.specs()
        } else {
            self.explicit.clone()
        };
        specs.sort_unstable();
        specs.dedup();
        specs
    }

    pub fn contains(&self, expected: NativeIndexSpec) -> bool {
        self.resolved().contains(&expected)
    }

    // VORTEX_RDF_WIRED_NATIVE_INDEX_SELECTION_V1
    /// Reject specifications whose proven builders are not native component
    /// producers yet. This prevents silently writing an incomplete artifact.
    pub fn ensure_materializable_now(&self) -> Result<()> {
        let unavailable: Vec<_> = self
            .resolved()
            .into_iter()
            .filter(|spec| {
                !matches!(
                    spec,
                    NativeIndexSpec::SubjectRangesV1 | NativeIndexSpec::PredicateRunsV1
                )
            })
            .map(|spec| spec.to_string())
            .collect();
        if unavailable.is_empty() {
            return Ok(());
        }
        Err(VortexRdfError::InvalidOperation(format!(
            "native index builders not integrated as transparent components yet: {}; use --native-index-profile bootstrap until the exact-range migration patches land",
            unavailable.join(", ")
        )))
    }
}

impl Default for NativeIndexSelection {
    fn default() -> Self {
        Self {
            profile: NativeIndexProfile::Standard,
            explicit: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_specs_override_profile_and_deduplicate() {
        let selection = NativeIndexSelection {
            profile: NativeIndexProfile::Standard,
            explicit: vec![
                NativeIndexSpec::PredicateRunsV1,
                NativeIndexSpec::PredicateRunsV1,
            ],
        };
        assert_eq!(selection.resolved(), vec![NativeIndexSpec::PredicateRunsV1]);
    }

    #[test]
    fn standard_profile_matches_proven_v9_index_family() {
        assert_eq!(
            NativeIndexProfile::Standard.specs(),
            vec![
                NativeIndexSpec::SubjectRangesV1,
                NativeIndexSpec::PredicateExactRangesV2,
                NativeIndexSpec::PredicateObjectExactRangesV2,
                NativeIndexSpec::ObjectExactRangesV2,
            ]
        );
    }

    #[test]
    fn specifications_parse_from_cli_form() {
        assert_eq!(
            "predicate-object:exact-ranges-v2"
                .parse::<NativeIndexSpec>()
                .unwrap(),
            NativeIndexSpec::PredicateObjectExactRangesV2
        );
        assert!("predicate:unknown".parse::<NativeIndexSpec>().is_err());
    }
}
