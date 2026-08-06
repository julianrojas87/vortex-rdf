//! The persistence model of a secondary index: how its `_idx_*` row-space
//! columns become standalone *components* and back again.
//!
//! One [`IndexComponent`] is one child of a native store file (or its
//! in-memory twin): a set of rows under the child's own plain column names,
//! carrying the writer's sortedness provenance. This module owns both
//! directions of that mapping — splitting a builder's welded row space into
//! components ([`split_built_row_space`]), describing the children a
//! serializer should write ([`index_component_specs`]), and reading a
//! persisted child's implementation slug back onto a component identity and
//! its row-space column names.
//!
//! The column *names* on either side belong to the index modules, not here:
//! everything below reaches them through
//! [`Family`](super::secondary_by_copy::Family) /
//! [`RefRole`](super::secondary_by_reference::RefRole) accessors or the
//! modules' own `push_component_specs`/`components_from_row_space`.

use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
#[cfg(feature = "file-io")]
use vortex_array::dtype::DType;
use vortex_array::dtype::FieldName;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};

use crate::error::{Result, VortexRdfError};
use crate::io::VORTEX_SESSION;

use super::secondary_by_copy::Family;
use super::secondary_by_reference::RefRole;
use super::{
    IndexType, Indexes, detect_indexes, secondary_by_copy, secondary_by_reference,
    strip_index_columns,
};

/// One secondary-index component held in memory beside the store's primary
/// base: the in-memory twin of a native store file's index child, carrying
/// the same rows under the same child schema (plain column names — `s`, `p`,
/// `o`, `g`, `rid` for a copy family; `val`, `rid` for a reference role).
///
/// `rid` values address rows of the base the component was built against;
/// that is what keeps components valid across derived views (a
/// `RowSelection` narrows without renumbering) and what invalidates them on
/// any physical gather.
#[derive(Clone)]
pub(crate) struct IndexComponent {
    /// The component name (`index:posg`, `index:ref-o`, …) — the same
    /// identity the persisted child carries.
    pub(crate) name: &'static str,
    /// The implementation slug (`secondary-by-copy/posg`, …).
    pub(crate) implementation: &'static str,
    /// The component's rows, canonicalized to one struct in child schema.
    pub(crate) array: StructArray,
    /// Whether the sort-key columns are GLOBALLY sorted — the writer's
    /// provenance, not an inspection: binary-search resolution is gated on
    /// this, and per-chunk-sorted data must never claim it (a false stamp
    /// corrupts query results; see the `sorted` field on the wire
    /// descriptor).
    pub(crate) sorted: bool,
}

impl IndexComponent {
    /// The component's rows as an `ArrayRef` (an `Arc` bump).
    pub(crate) fn as_array(&self) -> ArrayRef {
        self.array.clone().into_array()
    }

    /// Look a component up by name.
    pub(crate) fn find<'a>(
        components: &'a [IndexComponent],
        name: &str,
    ) -> Option<&'a IndexComponent> {
        components.iter().find(|c| c.name == name)
    }
}

/// The index set a component roster implies, in declaration (preference)
/// order — the in-memory counterpart of reading a file's child roster.
pub(crate) fn indexes_from_components(components: &[IndexComponent]) -> Indexes {
    let mut indexes: Indexes = Vec::new();
    for component in components {
        if let Some(index) = IndexType::from_component_slug(component.implementation)
            && !indexes.contains(&index)
        {
            indexes.push(index);
        }
    }
    indexes.sort_by_key(|index| index.preference_rank());
    indexes
}

/// Split a builder's welded row space into the store's model: the primary
/// quad array (index columns projected away) plus one [`IndexComponent`] per
/// persisted-child role the `_idx_*` columns assemble.
///
/// The builders keep emitting the welded form — it is also the streaming
/// write path's wire shape — and the store splits once at construction.
/// Sortedness is read off the welded columns' own `IsSorted` stamps, which
/// only the globally-sorted emission paths set (multi-chunk arrays lose
/// per-chunk stamps in canonicalization, correctly landing on `false`: a
/// concatenation of per-chunk sorts is not binary-searchable).
pub(crate) fn split_built_row_space(array: ArrayRef) -> Result<(ArrayRef, Vec<IndexComponent>)> {
    let detected = detect_indexes(array.dtype());
    if detected.is_empty() {
        return Ok((array, Vec::new()));
    }
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = array
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let mut components = Vec::new();
    for index in &detected {
        match index {
            IndexType::SecondaryByCopy => {
                secondary_by_copy::components_from_row_space(&struct_arr, &mut components)?
            }
            IndexType::SecondaryByReference => {
                secondary_by_reference::components_from_row_space(&struct_arr, &mut components)?
            }
        }
    }
    let primary = strip_index_columns(struct_arr.into_array())?;
    Ok((primary, components))
}

/// Rebuild a component from row-space column arrays: relabel
/// `source_columns[i]` (shared, stats and all) to the child's
/// `target_columns[i]` names. `sorted` provenance comes from the lead
/// column's own stamp — set only by globally-sorted emission.
pub(crate) fn component_from_columns(
    struct_arr: &StructArray,
    name: &'static str,
    implementation: &'static str,
    source_columns: &[&'static str],
    target_columns: &[&'static str],
    lead_source: &'static str,
) -> Result<Option<IndexComponent>> {
    let mut arrays = Vec::with_capacity(source_columns.len());
    for column in source_columns {
        match struct_arr.unmasked_field_by_name(column) {
            Ok(a) => arrays.push(a.clone()),
            // A partial column set means this component was never built.
            Err(_) => return Ok(None),
        }
    }
    let sorted = source_columns
        .iter()
        .position(|c| c == &lead_source)
        .map(|i| crate::store::array::column_is_sorted(&arrays[i]))
        .unwrap_or(false);
    let len = struct_arr.len();
    let array = StructArray::try_new(
        target_columns
            .iter()
            .map(|n| FieldName::from(*n))
            .collect::<Vec<_>>()
            .into(),
        arrays,
        len,
        Validity::NonNullable,
    )
    .map_err(VortexRdfError::Vortex)?;
    Ok(Some(IndexComponent {
        name,
        implementation,
        array,
        sorted,
    }))
}

/// One persisted child component of an index type: the descriptor written
/// into the native store root plus the row-space columns the component is
/// assembled from (`source_columns[i]` becomes the child's
/// `target_columns[i]`).
#[cfg(feature = "file-io")]
pub(crate) struct IndexComponentSpec {
    pub(crate) descriptor: crate::io::store_layout::StoreComponentDescriptor,
    pub(crate) source_columns: Vec<&'static str>,
    pub(crate) target_columns: Vec<&'static str>,
}

/// The index components a row-space dtype implies, over every detected index
/// type — what the serializer turns into auxiliary children. `sorted` is the
/// writer's promise that the emitted columns are globally (not per-chunk)
/// sorted; it travels to the descriptors so a reader lifting the components
/// back into memory knows whether they are binary-searchable.
#[cfg(feature = "file-io")]
pub(crate) fn index_component_specs(
    dtype: &DType,
    sorted: bool,
) -> Result<Vec<IndexComponentSpec>> {
    let mut specs = Vec::new();
    for index in detect_indexes(dtype) {
        match index {
            IndexType::SecondaryByCopy => {
                secondary_by_copy::push_component_specs(dtype, sorted, &mut specs)?
            }
            IndexType::SecondaryByReference => {
                secondary_by_reference::push_component_specs(dtype, sorted, &mut specs)?
            }
        }
    }
    Ok(specs)
}

/// The child struct dtype a component assembles: the row-space columns'
/// dtypes under the child's own names.
#[cfg(feature = "file-io")]
pub(crate) fn component_child_dtype(
    dtype: &DType,
    source_columns: &[&'static str],
    target_columns: &[&'static str],
) -> Result<DType> {
    use vortex_array::dtype::{Nullability, StructFields};
    let DType::Struct(fields, _) = dtype else {
        return Err(VortexRdfError::Serialization(format!(
            "index components need a struct row space, got {dtype}"
        )));
    };
    let field_dtypes = source_columns
        .iter()
        .map(|n| {
            fields.field(n).ok_or_else(|| {
                VortexRdfError::Serialization(format!("row space misses index column {n}"))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DType::Struct(
        StructFields::new(
            target_columns
                .iter()
                .map(|n| (*n).into())
                .collect::<Vec<std::sync::Arc<str>>>()
                .into(),
            field_dtypes,
        ),
        Nullability::NonNullable,
    ))
}

/// The canonical (name, implementation) identity of a known index component,
/// by implementation slug — how a reader adopts a persisted child as an
/// in-memory [`IndexComponent`]. `None` for foreign slugs (skippable when
/// optional).
pub(crate) fn component_identity_for_slug(
    implementation: &str,
) -> Option<(&'static str, &'static str)> {
    match implementation {
        x if x == secondary_by_copy::POSG_IMPLEMENTATION => Some((
            Family::Posg.component_name(),
            secondary_by_copy::POSG_IMPLEMENTATION,
        )),
        x if x == secondary_by_copy::OSPG_IMPLEMENTATION => Some((
            Family::Ospg.component_name(),
            secondary_by_copy::OSPG_IMPLEMENTATION,
        )),
        x if x == secondary_by_reference::O_IMPLEMENTATION => {
            Some((RefRole::O.component_name(), RefRole::O.component_slug()))
        }
        x if x == secondary_by_reference::P_IMPLEMENTATION => {
            Some((RefRole::P.component_name(), RefRole::P.component_slug()))
        }
        _ => None,
    }
}

/// The in-memory row-space column names a persisted index component's
/// columns re-glue into (positionally matching the child's column order) —
/// the read-side inverse of [`index_component_specs`].
pub(crate) fn row_space_columns_for_slug(implementation: &str) -> Option<Vec<&'static str>> {
    match implementation {
        secondary_by_copy::POSG_IMPLEMENTATION => Some(Family::Posg.column_names().to_vec()),
        secondary_by_copy::OSPG_IMPLEMENTATION => Some(Family::Ospg.column_names().to_vec()),
        secondary_by_reference::O_IMPLEMENTATION => {
            Some(vec![RefRole::O.val_col(), RefRole::O.rid_col()])
        }
        secondary_by_reference::P_IMPLEMENTATION => {
            Some(vec![RefRole::P.val_col(), RefRole::P.rid_col()])
        }
        _ => None,
    }
}
