//! Layout-feature test: chunk-probe bounds over a written vortex file must
//! agree with the canonical `partition_point` floor across chunk boundaries.
#![cfg(feature = "layout")]

use vortex_array::arrays::{PrimitiveArray, StructArray};
use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::session::ArraySession;
use vortex_array::stream::ArrayStreamAdapter;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};
use vortex_file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _};
use vortex_io::session::RuntimeSession;
use vortex_layout::session::LayoutSession;
use vortex_rdf_encoded_search::ColumnChunks;
use vortex_session::VortexSession;

fn session() -> VortexSession {
    use vortex_io::session::RuntimeSessionExt as _;
    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<LayoutSession>()
        .with::<ScalarFnSession>()
        .with::<RuntimeSession>()
        .with_tokio();
    vortex_file::register_default_encodings(&session);

    // The file writer is gated on the session's enabled editions; declare one
    // covering every registered array, layout and dtype plus the default
    // zone-map aggregates, as vortex-file's own tests do.
    use vortex_array::dtype::session::DTypeSessionExt as _;
    use vortex_array::session::ArraySessionExt as _;
    use vortex_edition::{
        ComponentKind, Edition, EditionId, EditionInclusion, EditionSessionExt as _,
    };
    use vortex_layout::session::LayoutSessionExt as _;
    const TEST_EDITION: EditionId = EditionId::new("test", 2026, 7, 0);
    let editions = session.editions();
    editions
        .declare_edition(Edition {
            id: TEST_EDITION,
            min_vortex_version: None,
        })
        .unwrap();
    let registered = [
        (
            ComponentKind::Array,
            session
                .arrays()
                .registry()
                .read(|map| map.keys().copied().collect::<Vec<_>>()),
        ),
        (
            ComponentKind::Layout,
            session
                .layouts()
                .registry()
                .read(|map| map.keys().copied().collect::<Vec<_>>()),
        ),
        (
            ComponentKind::DType,
            session
                .dtypes()
                .registry()
                .read(|map| map.keys().copied().collect::<Vec<_>>()),
        ),
    ];
    for (kind, ids) in &registered {
        for id in ids {
            editions
                .declare_inclusion(EditionInclusion::new(*kind, id, TEST_EDITION))
                .unwrap();
        }
    }
    for id in [
        "vortex.bounded_max",
        "vortex.bounded_min",
        "vortex.max",
        "vortex.min",
        "vortex.nan_count",
        "vortex.null_count",
    ] {
        editions
            .declare_inclusion(EditionInclusion::new(
                ComponentKind::Aggregate,
                id,
                TEST_EDITION,
            ))
            .unwrap();
    }
    session.enable_edition(TEST_EDITION).unwrap();
    session
}

fn chunk(data: &[u32]) -> ArrayRef {
    let s = PrimitiveArray::from_iter(data.iter().copied()).into_array();
    StructArray::try_new(["s"].into(), vec![s], data.len(), Validity::NonNullable)
        .unwrap()
        .into_array()
}

#[tokio::test(flavor = "multi_thread")]
async fn layout_chunks_probe_matches_canonical() {
    let session = session();

    // Sorted with 11-row runs, sized so the write strategy's coalescing still
    // yields multiple flat chunks; an equal run crosses every input boundary.
    let data: Vec<u32> = (0..600_000).map(|i| (i / 11) as u32).collect();
    let chunks: Vec<ArrayRef> = data.chunks(200_000).map(chunk).collect();
    let dtype = chunks[0].dtype().clone();

    let mut bytes = Vec::new();
    session
        .write_options()
        .write(
            &mut bytes,
            ArrayStreamAdapter::new(dtype, futures::stream::iter(chunks.into_iter().map(Ok))),
        )
        .await
        .unwrap();

    let file = session.open_options().open_buffer(bytes).unwrap();
    let root = file.footer().layout().clone();

    let column =
        ColumnChunks::from_struct_layout(&root, "s").expect("the written shape must resolve");
    assert!(ColumnChunks::from_struct_layout(&root, "absent").is_none());
    assert_eq!(column.row_count(), data.len() as u64);

    let source = file.segment_source();
    let canonical = |needle: u64| {
        let lo = data.partition_point(|&v| u64::from(v) < needle) as u64;
        let hi = data.partition_point(|&v| u64::from(v) <= needle) as u64;
        lo..hi.max(lo)
    };

    let max = u64::from(*data.last().unwrap());
    let mut needles: Vec<u64> = vec![0, 1, max, max + 1, u64::MAX];
    // Every ~997th distinct value and its neighbors, plus the values at the
    // input-chunk boundaries (where equal runs span chunks).
    needles.extend(
        (0..max)
            .step_by(997)
            .flat_map(|v| [v.saturating_sub(1), v, v + 1]),
    );
    for boundary in [200_000u64, 400_000] {
        let v = u64::from(data[boundary as usize]);
        needles.extend([v - 1, v, v + 1]);
    }
    for needle in needles {
        let got = column
            .bounds(needle, &source, &session)
            .await
            .unwrap()
            .expect("all chunks are probeable");
        assert_eq!(got, canonical(needle), "needle {needle}");
    }

    // Point reads across chunk interiors and boundaries need no sort order
    // and must agree with the source data exactly.
    let mut rows: Vec<u64> = (0..data.len() as u64).step_by(9973).collect();
    rows.extend([0, 199_999, 200_000, 399_999, 400_000, data.len() as u64 - 1]);
    for row in rows {
        let got = column
            .value_at(row, &source, &session)
            .await
            .unwrap()
            .expect("all chunks are probeable");
        assert_eq!(got, u64::from(data[row as usize]), "row {row}");
    }

    // Windowed bounds: search inside sub-ranges (including ones spanning a
    // chunk boundary) and compare against the canonical floor of the window.
    for window in [0u64..50_000, 150_000..250_000, 380_000..420_000, 5..6, 9..9] {
        let wdata = &data[window.start as usize..window.end as usize];
        for needle in [0u64, 3000, 13_600, 18_181, 36_363, u64::MAX] {
            let got = column
                .bounds_in(window.clone(), needle, &source, &session)
                .await
                .unwrap()
                .expect("all chunks are probeable");
            let lo = wdata.partition_point(|&v| u64::from(v) < needle) as u64;
            let hi = wdata.partition_point(|&v| u64::from(v) <= needle) as u64;
            assert_eq!(
                got,
                window.start + lo..window.start + hi,
                "window {window:?} needle {needle}"
            );
        }
    }
}

/// A struct chunk over two `u32` columns `p` and `o`.
fn struct_chunk(p: &[u32], o: &[u32]) -> ArrayRef {
    assert_eq!(p.len(), o.len());
    let len = p.len();
    let p = PrimitiveArray::from_iter(p.iter().copied()).into_array();
    let o = PrimitiveArray::from_iter(o.iter().copied()).into_array();
    StructArray::try_new(["p", "o"].into(), vec![p, o], len, Validity::NonNullable)
        .unwrap()
        .into_array()
}

/// A field's data node beneath its zoned wrappers, for shape assertions.
fn data_node(root: &vortex_layout::LayoutRef, field: &str) -> vortex_layout::LayoutRef {
    use vortex_layout::LayoutChildType;
    use vortex_layout::layouts::zoned::Zoned;
    let mut node = (0..root.nslots())
        .find_map(|i| {
            matches!(root.slot_type(i), Some(LayoutChildType::Field(ref name)) if name.as_ref() == field)
                .then(|| root.slot(i).ok().flatten())
                .flatten()
        })
        .expect("field present");
    while node.is::<Zoned>() {
        node = node.slot(0).unwrap().unwrap();
    }
    node
}

fn canonical_bounds(data: &[u32], needle: u64) -> std::ops::Range<u64> {
    let lo = data.partition_point(|&v| u64::from(v) < needle) as u64;
    let hi = data.partition_point(|&v| u64::from(v) <= needle) as u64;
    lo..hi.max(lo)
}

/// A column the default write strategy dictionary-encodes at the layout
/// level — its first block holds only a few distinct values — probes through
/// the codes leaves and the shared values leaf: global bounds over the sorted
/// lead column, point reads anywhere, and windowed bounds over a second
/// column sorted only within each lead run.
#[tokio::test(flavor = "multi_thread")]
async fn layout_chunks_probe_dictionary_coded_columns() {
    use vortex_layout::layouts::dict::Dict as DictLayout;

    let session = session();
    // Three lead runs of 3,000 rows; the second column restarts inside each.
    let p: Vec<u32> = (0..9_000).map(|i| (i / 3_000) as u32).collect();
    let o: Vec<u32> = (0..9_000).map(|i| ((i % 3_000) / 300) as u32).collect();
    let chunk = struct_chunk(&p, &o);
    let dtype = chunk.dtype().clone();

    let mut bytes = Vec::new();
    session
        .write_options()
        .write(
            &mut bytes,
            ArrayStreamAdapter::new(dtype, futures::stream::iter([Ok(chunk)])),
        )
        .await
        .unwrap();
    let file = session.open_options().open_buffer(bytes).unwrap();
    let root = file.footer().layout().clone();
    for field in ["p", "o"] {
        assert!(
            data_node(&root, field).is::<DictLayout>(),
            "the writer no longer dictionary-encodes `{field}`; the fixture must change to keep covering the dict layout"
        );
    }

    let source = file.segment_source();
    let pc = ColumnChunks::from_struct_layout(&root, "p").expect("a dict layout resolves");
    let oc = ColumnChunks::from_struct_layout(&root, "o").expect("a dict layout resolves");
    assert_eq!(pc.row_count(), 9_000);
    assert_eq!(oc.row_count(), 9_000);

    for needle in [0u64, 1, 2, 3, 17, u64::MAX] {
        let got = pc
            .bounds(needle, &source, &session)
            .await
            .unwrap()
            .expect("dictionary-coded leaves are probeable");
        assert_eq!(got, canonical_bounds(&p, needle), "needle {needle}");
    }

    let mut rows: Vec<u64> = (0..9_000u64).step_by(97).collect();
    rows.extend([0, 2_999, 3_000, 5_999, 6_000, 8_999]);
    for row in rows {
        for (column, data) in [(&pc, &p), (&oc, &o)] {
            let got = column
                .value_at(row, &source, &session)
                .await
                .unwrap()
                .expect("dictionary-coded leaves are probeable");
            assert_eq!(got, u64::from(data[row as usize]), "row {row}");
        }
    }

    for run in 0..3u64 {
        let window = run * 3_000..(run + 1) * 3_000;
        let wdata = &o[window.start as usize..window.end as usize];
        for needle in (0..=10u64).chain([u64::MAX]) {
            let got = oc
                .bounds_in(window.clone(), needle, &source, &session)
                .await
                .unwrap()
                .expect("dictionary-coded leaves are probeable");
            let want = canonical_bounds(wdata, needle);
            assert_eq!(
                got,
                window.start + want.start..window.start + want.end,
                "window {window:?} needle {needle}"
            );
        }
    }
}

/// A column whose dictionary outgrows its constraints becomes a chunked run
/// of dictionary layouts, each with its own values leaf and chunked codes;
/// probes address rows across dictionary boundaries exactly.
#[tokio::test(flavor = "multi_thread")]
async fn layout_chunks_probe_run_of_dictionaries() {
    use std::sync::Arc;

    use vortex_btrblocks::BtrBlocksCompressorBuilder;
    use vortex_layout::layouts::chunked::Chunked as ChunkedLayout;
    use vortex_layout::layouts::chunked::writer::ChunkedLayoutStrategy;
    use vortex_layout::layouts::compressed::CompressorPlugin;
    use vortex_layout::layouts::dict::Dict as DictLayout;
    use vortex_layout::layouts::dict::writer::{
        DictLayoutConstraints, DictLayoutOptions, DictStrategy,
    };
    use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
    use vortex_layout::layouts::struct_::StructStrategy;

    let session = session();
    // Forty distinct values of 500 rows each, fed as four input chunks; a
    // dictionary holds at most sixteen values, so the column becomes three
    // dictionaries whose codes leaves follow the input chunking.
    let data: Vec<u32> = (0..20_000).map(|i| (i / 500) as u32).collect();
    let chunks: Vec<ArrayRef> = data.chunks(5_000).map(chunk).collect();
    let dtype = chunks[0].dtype().clone();
    let compressor: Arc<dyn CompressorPlugin> =
        Arc::new(BtrBlocksCompressorBuilder::default().build());
    let dict = DictStrategy::new(
        ChunkedLayoutStrategy::new(FlatLayoutStrategy::default()),
        FlatLayoutStrategy::default(),
        FlatLayoutStrategy::default(),
        DictLayoutOptions {
            constraints: DictLayoutConstraints {
                max_len: 16,
                ..Default::default()
            },
        },
        compressor,
    );
    let strategy = StructStrategy::new(Arc::new(FlatLayoutStrategy::default()), Arc::new(dict));

    let mut bytes = Vec::new();
    session
        .write_options()
        .with_strategy(Arc::new(strategy))
        .write(
            &mut bytes,
            ArrayStreamAdapter::new(dtype, futures::stream::iter(chunks.into_iter().map(Ok))),
        )
        .await
        .unwrap();
    let file = session.open_options().open_buffer(bytes).unwrap();
    let root = file.footer().layout().clone();
    let node = data_node(&root, "s");
    assert!(
        node.is::<ChunkedLayout>() && node.nslots() == 3,
        "expected a chunked run of three dictionaries, got {}",
        node.display_tree()
    );
    for i in 0..node.nslots() {
        assert!(node.slot(i).unwrap().unwrap().is::<DictLayout>());
    }

    let source = file.segment_source();
    let column =
        ColumnChunks::from_struct_layout(&root, "s").expect("a run of dictionaries resolves");
    assert_eq!(column.row_count(), 20_000);

    for needle in (0..=40u64).chain([100, u64::MAX]) {
        let got = column
            .bounds(needle, &source, &session)
            .await
            .unwrap()
            .expect("dictionary-coded leaves are probeable");
        assert_eq!(got, canonical_bounds(&data, needle), "needle {needle}");
    }

    // Rows around every dictionary boundary (values 15→16 and 31→32) and the
    // input chunk boundaries, plus a stride over the interior.
    let mut rows: Vec<u64> = (0..20_000u64).step_by(251).collect();
    rows.extend([
        0, 4_999, 5_000, 7_999, 8_000, 9_999, 10_000, 15_999, 16_000, 19_999,
    ]);
    for row in rows {
        let got = column
            .value_at(row, &source, &session)
            .await
            .unwrap()
            .expect("dictionary-coded leaves are probeable");
        assert_eq!(got, u64::from(data[row as usize]), "row {row}");
    }

    for window in [7_000u64..9_000, 15_500..16_500, 0..20_000, 4_900..5_100] {
        let wdata = &data[window.start as usize..window.end as usize];
        for needle in [0u64, 14, 15, 16, 17, 31, 32, 39, 40, u64::MAX] {
            let got = column
                .bounds_in(window.clone(), needle, &source, &session)
                .await
                .unwrap()
                .expect("dictionary-coded leaves are probeable");
            let want = canonical_bounds(wdata, needle);
            assert_eq!(
                got,
                window.start + want.start..window.start + want.end,
                "window {window:?} needle {needle}"
            );
        }
    }
}
