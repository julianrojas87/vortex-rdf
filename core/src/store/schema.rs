//! The column names that define the serialized format.
//!
//! These are contract, not policy: a name change here changes what a written
//! file means, so they live in one place. Column names owned by a single
//! subsystem live with that subsystem instead: index column names (`_idx_*`)
//! in their index modules ([`secondary_by_copy::Family`],
//! [`secondary_by_reference`]), the dictionary child's `_dict_term` in
//! [`term_dictionary`](crate::store::term_dictionary).
//!
//! [`secondary_by_copy::Family`]: crate::store::indexes::secondary_by_copy::Family
//! [`secondary_by_reference`]: crate::store::indexes::secondary_by_reference

/// The subject column — first in every layout, globally sorted by the sorted
/// builders (which stamp `IsSorted` on it for the binary-search fast path).
pub(crate) const COL_S: &str = "s";
/// The predicate column.
pub(crate) const COL_P: &str = "p";
/// The object column (`o_value` under the TypedObject layout's split form).
pub(crate) const COL_O: &str = "o";
/// The graph-name column (empty string = default graph).
pub(crate) const COL_G: &str = "g";

/// The four primary columns in emission order.
pub(crate) const PRIMARY_COLUMNS: [&str; 4] = [COL_S, COL_P, COL_O, COL_G];

/// The TypedObject layout's split object columns: the term kind tag (its
/// presence in a schema is what marks the layout), the lexical value, and the
/// literal datatype/language columns (empty when not applicable).
pub(crate) const COL_O_KIND: &str = "o_kind";
pub(crate) const COL_O_VALUE: &str = "o_value";
pub(crate) const COL_O_DATATYPE: &str = "o_datatype";
pub(crate) const COL_O_LANG: &str = "o_lang";
