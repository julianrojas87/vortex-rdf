//! Typed residual equality filtering: the row-at-a-time fast paths
//! `match_pattern` uses instead of the vectorized mask pipeline when every
//! residual constraint binds to a canonical column — u32 code compares for the
//! Dictionary layout's columns, view-level string compares for the Default /
//! TypedObject / tail string columns. Anything else declines, and the caller
//! falls back to the general mask-scan pipeline.

use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{Primitive, PrimitiveArray, StructArray, VarBinView, VarBinViewArray};
use vortex_array::scalar::Scalar;
use vortex_array::{ArrayRef, VortexSessionExecute};

use crate::io::VORTEX_LIGHT_SESSION;

/// A residual equality constraint's probe value, extracted from its `Scalar`
/// once per scan — not per chunk, the string extraction allocates.
pub(crate) enum Needle {
    Code(u32),
    Str(String),
}

impl Needle {
    fn from_scalar(scalar: &Scalar) -> Option<Self> {
        if let Ok(code) = u32::try_from(scalar) {
            return Some(Needle::Code(code));
        }
        Some(Needle::Str(scalar.as_utf8_opt()?.value()?.to_string()))
    }

    /// Extract every constraint's probe once; `None` if any is neither a u32
    /// code nor a utf8 string.
    pub(crate) fn extract(eqs: &[(&'static str, Scalar)]) -> Option<Vec<Needle>> {
        if eqs.is_empty() {
            return None;
        }
        eqs.iter().map(|(_, s)| Needle::from_scalar(s)).collect()
    }
}

/// One equality constraint bound to a concrete typed column view, for the
/// typed residual-filter fast paths. Two column shapes qualify: canonical
/// non-nullable u32 primitives (the Dictionary layout's code columns, compared
/// as integers) and canonical non-nullable Utf8 `VarBinView`s (the Default /
/// TypedObject / tail string columns, compared at the view level). Anything
/// else — nullable, compressed, or chunked — declines, and the caller falls
/// back to the general mask-scan pipeline.
pub(crate) enum TypedEq<'a> {
    Code(PrimitiveArray, u32),
    Str(StrEq<'a>),
}

/// A string equality probe over a canonical `VarBinView` column, comparing at
/// the view level: length first (a u32 read from the 16-byte view struct —
/// which alone rejects almost every row), then the inline bytes or the
/// referenced buffer range. No per-row `ByteBuffer` is materialized —
/// profiling showed `bytes_at`'s slice/refcount traffic dominating tail scans.
pub(crate) struct StrEq<'a> {
    arr: VarBinViewArray,
    needle: &'a [u8],
}

impl StrEq<'_> {
    #[inline]
    fn matches(&self, i: usize) -> bool {
        let view = &self.arr.views()[i];
        if view.len() as usize != self.needle.len() {
            return false;
        }
        if view.is_inlined() {
            view.as_inlined().value() == self.needle
        } else {
            let r = view.as_view();
            let buf: &[u8] = self.arr.buffer(r.buffer_index as usize);
            &buf[r.as_range()] == self.needle
        }
    }
}

impl<'a> TypedEq<'a> {
    fn bind_col(
        col: &ArrayRef,
        needle: &'a Needle,
        ctx: &mut vortex_array::ExecutionCtx,
    ) -> Option<TypedEq<'a>> {
        use vortex_array::dtype::DType;
        if col.dtype().is_nullable() {
            return None;
        }
        match needle {
            Needle::Code(code) => {
                if !col.dtype().is_unsigned_int() {
                    return None;
                }
                // A struct canonicalization does not recurse into its children,
                // so a code column stored under a compression (BtrBlocks) is not
                // a bare `Primitive` yet. Try the cheap downcast first — the
                // Dictionary layout's codes are already canonical, the hot case —
                // and only pay a one-off `execute` to decode an encoded column.
                let prim = match col.clone().try_downcast::<Primitive>() {
                    Ok(p) => p,
                    Err(_) => col.clone().execute::<PrimitiveArray>(ctx).ok()?,
                };
                if prim.ptype() != vortex_array::dtype::PType::U32 {
                    return None;
                }
                Some(TypedEq::Code(prim, *code))
            }
            Needle::Str(s) => {
                if !matches!(col.dtype(), DType::Utf8(_)) {
                    return None;
                }
                // Same as the code arm: the Default / TypedObject layouts hold
                // their string columns BtrBlocks-encoded, so the direct downcast
                // fails and they used to fall to the general mask-scan pipeline (a
                // per-column `compare_views_constant` over the whole array, then a
                // boolean AND — no per-row short-circuit). Decoding once to
                // canonical views here lets `StrEq` run the length-first,
                // conjunction-short-circuiting loop instead. Only reached with ≥2
                // constraints (see `VortexRdfStore::typed_residual_ids`), where
                // the short-circuit repays the decode.
                let arr = match col.clone().try_downcast::<VarBinView>() {
                    Ok(a) => a,
                    Err(_) => col.clone().execute::<VarBinViewArray>(ctx).ok()?,
                };
                Some(TypedEq::Str(StrEq {
                    arr,
                    needle: s.as_bytes(),
                }))
            }
        }
    }

    /// Bind every constraint to its typed column, or `None` if any declines.
    pub(crate) fn bind(
        struct_arr: &StructArray,
        eqs: &[(&'static str, Scalar)],
        needles: &'a [Needle],
        ctx: &mut vortex_array::ExecutionCtx,
    ) -> Option<Vec<TypedEq<'a>>> {
        let mut cols = Vec::with_capacity(eqs.len());
        for ((field, _), needle) in eqs.iter().zip(needles) {
            let col = struct_arr.unmasked_field_by_name(field).ok()?;
            cols.push(TypedEq::bind_col(col, needle, ctx)?);
        }
        Some(cols)
    }

    /// The mixed/cold row compare. The per-row `as_slice` on the Code arm is
    /// deliberate: mixed code+string constraint sets do not occur in practice
    /// (Dictionary bases are all-code, tails and Default bases all-string),
    /// and the hot all-code case takes [`TypedEq::code_views`] instead.
    #[inline]
    pub(crate) fn matches(&self, i: usize) -> bool {
        match self {
            TypedEq::Code(prim, code) => prim.as_slice::<u32>()[i] == *code,
            TypedEq::Str(s) => s.matches(i),
        }
    }

    /// The all-code specialization: when every constraint is a u32 code
    /// compare (the Dictionary layout), the row loop over plain
    /// `(&[u32], u32)` pairs — slices hoisted once, borrowing from the bound
    /// constraints — is branch-free per constraint and vectorizes;
    /// benchmarking showed a mixed-enum loop costing ~2× on full-column
    /// scans. `None` when any constraint is a string compare.
    pub(crate) fn code_views<'b>(cols: &'b [TypedEq<'a>]) -> Option<Vec<(&'b [u32], u32)>> {
        cols.iter()
            .map(|c| match c {
                TypedEq::Code(prim, code) => Some((prim.as_slice::<u32>(), *code)),
                TypedEq::Str(..) => None,
            })
            .collect()
    }
}

/// The positions (in `applied`'s own row order) matching every constraint,
/// via the typed comparisons of [`TypedEq`] — the tail counterpart of
/// `VortexRdfStore::typed_residual_ids`. Accepts a flat canonical struct or
/// a chunked accretion of them (the shape `VortexRdfStore::add_quads`
/// builds); `None` on any other shape, falling back to the mask pipeline.
pub(crate) fn typed_positions(
    applied: &ArrayRef,
    eqs: &[(&'static str, Scalar)],
) -> Option<Vec<usize>> {
    use vortex_array::arrays::chunked::ChunkedArrayExt;
    use vortex_array::arrays::{Chunked, Struct};
    if eqs.is_empty() {
        return None;
    }
    let needles = Needle::extract(eqs)?;
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
    fn positions_of(
        sa: &StructArray,
        eqs: &[(&'static str, Scalar)],
        needles: &[Needle],
        offset: usize,
        out: &mut Vec<usize>,
        ctx: &mut vortex_array::ExecutionCtx,
    ) -> Option<()> {
        let cols = TypedEq::bind(sa, eqs, needles, ctx)?;
        if let Some(codes) = TypedEq::code_views(&cols) {
            out.extend(
                (0..sa.len())
                    .filter(|&i| codes.iter().all(|(s, c)| s[i] == *c))
                    .map(|i| offset + i),
            );
        } else {
            out.extend(
                (0..sa.len())
                    .filter(|&i| cols.iter().all(|c| c.matches(i)))
                    .map(|i| offset + i),
            );
        }
        Some(())
    }
    let mut out = Vec::new();
    if let Ok(sa) = applied.clone().try_downcast::<Struct>() {
        positions_of(&sa, eqs, &needles, 0, &mut out, &mut ctx)?;
        return Some(out);
    }
    if let Ok(ch) = applied.clone().try_downcast::<Chunked>() {
        let mut offset = 0usize;
        for chunk in ch.chunks() {
            let sa = chunk.try_downcast::<Struct>().ok()?;
            positions_of(&sa, eqs, &needles, offset, &mut out, &mut ctx)?;
            offset += sa.len();
        }
        return Some(out);
    }
    None
}
