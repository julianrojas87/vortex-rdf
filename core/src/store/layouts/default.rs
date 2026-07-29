//! Column-building and decoding logic for [`LayoutStrategy::Default`]:
//! all four quad fields stored as opaque UTF-8 strings in N-Triples form.
//!
//! [`LayoutStrategy::Default`]: super::LayoutStrategy::Default

use std::sync::Arc;

use oxrdf::Quad;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::builders::VarBinViewBuilder;
use vortex_array::dtype::{DType, Nullability};
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};

use crate::common::array::{StrColReader, make_string_array};
use crate::common::terms::{get_as_term, parse_graph_name, parse_named_node, parse_subject};
use crate::error::{Result, VortexRdfError};
use crate::io::VORTEX_LIGHT_SESSION;
use crate::store::RawQuad;
use crate::store::schema::{COL_G, COL_O, COL_P, COL_S, PRIMARY_COLUMNS};

/// Field names of the primary columns: `s`, `p`, `o`, `g`.
pub(crate) fn field_names() -> Vec<Arc<str>> {
    PRIMARY_COLUMNS.iter().map(|&n| n.into()).collect()
}

/// Build the primary column arrays from raw quads. An empty slice yields
/// empty columns with the correct dtypes.
pub(crate) fn build_columns(quads: &[RawQuad]) -> Vec<ArrayRef> {
    vec![
        make_string_array(quads.iter().map(|q| q.s.as_str())),
        make_string_array(quads.iter().map(|q| q.p.as_str())),
        make_string_array(quads.iter().map(|q| q.o.as_str())),
        make_string_array(quads.iter().map(|q| q.g.as_str())),
    ]
}

/// Decode a StructArray chunk with `s`/`p`/`o`/`g` string columns into Quads.
pub(crate) fn decode_chunk(chunk: &ArrayRef) -> Vec<Result<Quad>> {
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();

    let struct_arr = match chunk.clone().execute::<StructArray>(&mut ctx) {
        Ok(a) => a,
        Err(e) => return vec![Err(VortexRdfError::Vortex(e))],
    };

    let n = struct_arr.len();

    macro_rules! get_str_col {
        ($name:expr) => {
            match struct_arr
                .unmasked_field_by_name($name)
                .map_err(VortexRdfError::Vortex)
                .and_then(|c| {
                    c.clone()
                        .execute::<VarBinViewArray>(&mut ctx)
                        .map_err(VortexRdfError::Vortex)
                }) {
                Ok(arr) => arr,
                Err(e) => return vec![Err(e)],
            }
        };
    }

    let s_col = get_str_col!(COL_S);
    let p_col = get_str_col!(COL_P);
    let o_col = get_str_col!(COL_O);
    let g_col = get_str_col!(COL_G);

    let s = StrColReader::new(&s_col);
    let p = StrColReader::new(&p_col);
    let o = StrColReader::new(&o_col);
    let g = StrColReader::new(&g_col);

    (0..n)
        .map(|i| {
            // Borrow &str views over the column buffers (zero-copy);
            // the oxrdf constructors make the single owned copy.
            decode_spog(s.str_at(i)?, p.str_at(i)?, o.str_at(i)?, g.str_at(i)?)
        })
        .collect()
}

pub(crate) fn decode_spog(s: &str, p: &str, o: &str, g: &str) -> Result<Quad> {
    let subject = parse_subject(s)?;
    let predicate = parse_named_node(p)?;
    let object = get_as_term(o)
        .ok_or_else(|| VortexRdfError::Deserialization(format!("Invalid object: {}", o)))?;
    let graph_name = parse_graph_name(g)?;
    Ok(Quad::new(subject, predicate, object, graph_name))
}

/// Column builders for the Default layout, filled directly from quads.
///
/// The quad's terms are already in the N-Triples form the columns store, so
/// they are appended straight into the column builders: steady-state ingestion
/// performs no per-quad heap allocations and no formatting at all.
pub(crate) struct DirectChunkBuilder {
    s: VarBinViewBuilder,
    p: VarBinViewBuilder,
    o: VarBinViewBuilder,
    g: VarBinViewBuilder,
    len: usize,
}

impl DirectChunkBuilder {
    pub(crate) fn new(capacity: usize) -> Self {
        let col =
            || VarBinViewBuilder::with_capacity(DType::Utf8(Nullability::NonNullable), capacity);
        Self {
            s: col(),
            p: col(),
            o: col(),
            g: col(),
            len: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn push(&mut self, q: &RawQuad) {
        self.s.append_value(&q.s);
        self.p.append_value(&q.p);
        self.o.append_value(&q.o);
        // The default graph is already the empty string in `RawQuad`.
        self.g.append_value(&q.g);
        self.len += 1;
    }

    pub(crate) fn finish(mut self) -> Result<ArrayRef> {
        StructArray::try_new(
            field_names().into(),
            vec![
                self.s.finish_into_varbinview().into_array(),
                self.p.finish_into_varbinview().into_array(),
                self.o.finish_into_varbinview().into_array(),
                self.g.finish_into_varbinview().into_array(),
            ],
            self.len,
            Validity::NonNullable,
        )
        .map_err(VortexRdfError::Vortex)
        .map(|a| a.into_array())
    }
}
