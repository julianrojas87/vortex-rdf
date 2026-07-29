//! Shared correctness primitives for native exact-range indexes.
//!
//! Physical key ordering and spill formats remain index-specific. This module
//! owns only invariants common to predicate, predicate-object, and object
//! directory/payload implementations.

use crate::error::{Result, VortexRdfError};
use std::ops::Range;

// VORTEX_RDF_SHARED_NATIVE_EXACT_RANGE_PRIMITIVES_V1
pub(crate) fn range_rows(ranges: &[Range<u64>]) -> u64 {
    ranges
        .iter()
        .map(|range| range.end.saturating_sub(range.start))
        .sum()
}

pub(crate) fn checked_payload_end(
    range_offset: u64,
    range_count: u32,
    context: &str,
) -> Result<u64> {
    range_offset
        .checked_add(u64::from(range_count))
        .ok_or_else(|| VortexRdfError::Deserialization(format!("{context} payload range overflow")))
}

pub(crate) fn validate_exact_ranges(
    ranges: &[Range<u64>],
    expected_candidate_rows: u64,
    context: &str,
) -> Result<()> {
    let mut previous_end = None;
    for range in ranges {
        if range.start >= range.end {
            return Err(VortexRdfError::Deserialization(format!(
                "{context} contains invalid range {}..{}",
                range.start, range.end
            )));
        }
        if previous_end.is_some_and(|end| range.start < end) {
            return Err(VortexRdfError::Deserialization(format!(
                "{context} contains overlapping or unsorted ranges"
            )));
        }
        previous_end = Some(range.end);
    }
    let actual_rows = range_rows(ranges);
    if actual_rows != expected_candidate_rows {
        return Err(VortexRdfError::Deserialization(format!(
            "{context} candidate-row mismatch: expected={expected_candidate_rows}, actual={actual_rows}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_sorted_disjoint_ranges_and_candidate_rows() {
        let ranges = vec![2..5, 9..11];
        assert_eq!(range_rows(&ranges), 5);
        assert!(validate_exact_ranges(&ranges, 5, "fixture").is_ok());
    }

    #[test]
    fn rejects_empty_overlapping_unsorted_and_mismatched_ranges() {
        assert!(validate_exact_ranges(&[3..3], 0, "fixture").is_err());
        assert!(validate_exact_ranges(&[1..4, 3..5], 5, "fixture").is_err());
        assert!(validate_exact_ranges(&[5..7, 1..2], 3, "fixture").is_err());
        assert!(validate_exact_ranges(&[1..3], 3, "fixture").is_err());
    }

    #[test]
    fn payload_end_is_checked() {
        assert_eq!(checked_payload_end(7, 3, "fixture").unwrap(), 10);
        assert!(checked_payload_end(u64::MAX, 1, "fixture").is_err());
    }
}
