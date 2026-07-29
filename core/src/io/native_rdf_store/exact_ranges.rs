//! Shared correctness primitives for native exact-range indexes.
//!
//! Physical key ordering and spill formats remain index-specific. This module
//! owns only invariants common to predicate, predicate-object, and object
//! directory/payload implementations.

use crate::error::{Result, VortexRdfError};
use std::ops::Range;
use vortex_array::arrays::{PrimitiveArray, StructArray};
use vortex_array::{ArrayRef, IntoArray};

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

pub(crate) fn build_exact_range_payload_array(
    starts: Vec<u64>,
    ends: Vec<u64>,
) -> Result<ArrayRef> {
    if starts.len() != ends.len() {
        return Err(VortexRdfError::Serialization(
            "exact-range payload columns have different lengths".into(),
        ));
    }
    StructArray::from_fields(&[
        ("row_start", PrimitiveArray::from_iter(starts).into_array()),
        ("row_end", PrimitiveArray::from_iter(ends).into_array()),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|array| array.into_array())
}

pub(crate) fn build_exact_range_directory_array(
    key_name: &'static str,
    keys: Vec<u32>,
    offsets: Vec<u64>,
    counts: Vec<u32>,
    candidate_rows: Vec<u64>,
) -> Result<ArrayRef> {
    let rows = keys.len();
    if offsets.len() != rows || counts.len() != rows || candidate_rows.len() != rows {
        return Err(VortexRdfError::Serialization(format!(
            "{key_name} exact-range directory columns have different lengths"
        )));
    }
    StructArray::from_fields(&[
        (key_name, PrimitiveArray::from_iter(keys).into_array()),
        (
            "range_offset",
            PrimitiveArray::from_iter(offsets).into_array(),
        ),
        (
            "range_count",
            PrimitiveArray::from_iter(counts).into_array(),
        ),
        (
            "candidate_rows",
            PrimitiveArray::from_iter(candidate_rows).into_array(),
        ),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|array| array.into_array())
}

// VORTEX_RDF_SHARED_EXACT_RANGE_PAYLOAD_DECODER_V1
pub(crate) fn decode_exact_range_columns(
    starts: Vec<u64>,
    ends: Vec<u64>,
    expected_range_count: u32,
    expected_candidate_rows: u64,
    context: &str,
) -> Result<Vec<Range<u64>>> {
    let expected = usize::try_from(expected_range_count).map_err(|_| {
        VortexRdfError::Deserialization(format!("{context} range count exceeds usize"))
    })?;
    if starts.len() != expected || ends.len() != expected {
        return Err(VortexRdfError::Deserialization(format!(
            "{context} payload length mismatch: starts={}, ends={}, expected={expected}",
            starts.len(),
            ends.len()
        )));
    }
    let ranges: Vec<_> = starts
        .into_iter()
        .zip(ends)
        .map(|(start, end)| start..end)
        .collect();
    validate_exact_ranges(&ranges, expected_candidate_rows, context)?;
    Ok(ranges)
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
    fn shared_arrays_preserve_stable_schemas() {
        let payload = build_exact_range_payload_array(vec![1], vec![3]).unwrap();
        let directory =
            build_exact_range_directory_array("object_id", vec![7], vec![0], vec![1], vec![2])
                .unwrap();
        assert_eq!(payload.len(), 1);
        assert_eq!(directory.len(), 1);
        assert!(build_exact_range_payload_array(vec![1], Vec::new()).is_err());
    }

    #[test]
    fn payload_decoder_checks_lengths_order_and_rows() {
        assert_eq!(
            decode_exact_range_columns(vec![1, 5], vec![3, 6], 2, 3, "fixture").unwrap(),
            vec![1..3, 5..6]
        );
        assert!(decode_exact_range_columns(vec![1], vec![], 1, 1, "fixture").is_err());
        assert!(decode_exact_range_columns(vec![3], vec![2], 1, 0, "fixture").is_err());
    }

    #[test]
    fn payload_end_is_checked() {
        assert_eq!(checked_payload_end(7, 3, "fixture").unwrap(), 10);
        assert!(checked_payload_end(u64::MAX, 1, "fixture").is_err());
    }
}
