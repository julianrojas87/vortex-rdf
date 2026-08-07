//! The build strategy that keeps quads in arrival order, emitting each chunk
//! as it fills.
//!
//! It is the cheapest path to a store and the one that promises the least:
//! the quad columns are never globally sorted, so this module must never
//! stamp `IsSorted` on `s`. Index columns are sorted per chunk, and may claim
//! global sortedness only when the build is a single whole-dataset chunk.
//!
//! Memory is O(chunk), with one exception the Dictionary layout forces: no
//! chunk can be encoded before the term dictionary is complete, so that
//! layout runs a two-pass pipeline — buffered in memory up to one chunk,
//! spilling to a `spill` run only when the dataset outgrows it (streaming
//! builds) — or interns into one materialized chunk (in-memory builds).

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use super::spill::{RunReader, RunWriter, TempRunsGuard, make_temp_dir};
use super::{
    BuiltArray, BuiltStream, ChunkStream, DEFAULT_CHUNK_SIZE, VortexArrayBuilder, assemble_chunks,
    build_struct_array, into_vortex_error, make_empty_struct,
};
use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;
use crate::store::indexes::Indexes;
use crate::store::layouts::default::DirectChunkBuilder;
use crate::store::layouts::dictionary::{TermDictionaryBuilder, ingest_interning};
use crate::store::layouts::{LayoutStrategy, dictionary};

use futures::{Stream, StreamExt, TryStreamExt, stream};
use std::sync::Arc;
use web_time::Instant;

use vortex_array::ArrayRef;
use vortex_array::dtype::DType;

/// Unsorted Vortex RDF Array Builder.
///
/// Quads are ingested in natural insertion order and built into fixed-size
/// StructArray chunks:
///
/// - `build_vortex_stream` (used when serializing to a file) produces chunks
///   lazily as the Vortex writer polls for them, so peak memory is bounded by
///   the chunk size rather than the dataset size.
/// - `build_vortex_array` (used for in-memory stores) collects the same chunks
///   into a single (possibly chunked) array.
pub struct UnsortedStreamBuilder;

impl VortexArrayBuilder for UnsortedStreamBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltArray> {
        let start = Instant::now();

        // Dictionary layout: the result is materialized anyway, so intern
        // terms as the stream drains (each unique term held once, 16 bytes
        // per quad — no `Vec<RawQuad>` accumulates) and build one contiguous
        // chunk. Index columns are globally sorted and stamped for
        // binary-search routing; the quads keep arrival order.
        if layout == LayoutStrategy::Dictionary {
            let (dict, codes) = ingest_interning(quad_stream).await?.finish(false)?;
            let array = dictionary::build_array(&codes, &indexes, false)?;
            log::debug!(
                "[UnsortedStreamBuilder] Materialized {} dictionary-encoded quads in {:?}",
                array.len(),
                start.elapsed()
            );
            return Ok(BuiltArray {
                array,
                components: Vec::new(),
                dict: Some(Arc::new(dict)),
            });
        }

        let built =
            build_chunk_stream(quad_stream, layout, indexes.clone(), DEFAULT_CHUNK_SIZE).await?;
        let chunks: Vec<ArrayRef> = built
            .chunks
            .try_collect()
            .await
            .map_err(VortexRdfError::Vortex)?;

        let result = assemble_chunks(chunks, layout, &indexes)?;
        log::debug!(
            "[UnsortedStreamBuilder] Materialized {} quads in {:?}",
            result.len(),
            start.elapsed()
        );
        Ok(BuiltArray {
            array: result,
            components: Vec::new(),
            dict: built.dict,
        })
    }

    /// True streaming implementation: chunks are built on demand as the file
    /// writer polls the stream.
    async fn build_vortex_stream(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltStream> {
        build_chunk_stream(quad_stream, layout, indexes, DEFAULT_CHUNK_SIZE).await
    }
}

/// Produce the schema dtype and a lazily-evaluated stream of StructArray chunks.
///
/// The first chunk is read eagerly because the Vortex writer needs the schema
/// dtype before the first chunk arrives (and it surfaces input errors early).
/// Subsequent chunks are built only when the consumer polls for them, each
/// carrying global row IDs via `start_row` so index columns stay valid across
/// the assembled file.
pub(crate) async fn build_chunk_stream(
    quads: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<BuiltStream> {
    // Dictionary layout: the global dictionary must be complete before any
    // encoded chunk can be emitted, so this runs a two-pass spill pipeline.
    if layout == LayoutStrategy::Dictionary {
        return build_dict_chunk_stream(quads, indexes, chunk_size).await;
    }
    // Fast path: with the Default layout and no index columns, terms are
    // formatted straight into the column builders — no intermediate RawQuad
    // strings are allocated.
    let (dtype, chunks, components_sorted) =
        if layout == LayoutStrategy::Default && indexes.is_empty() {
            let (dtype, chunks) = build_direct_chunk_stream(quads, chunk_size).await?;
            (dtype, chunks, false)
        } else {
            build_buffered_chunk_stream(quads, layout, indexes, chunk_size).await?
        };
    Ok(BuiltStream {
        components: Vec::new(),
        // A single whole-dataset chunk carries globally sorted, stamped
        // index columns; anything longer is per-chunk local sorts.
        components_sorted,
        quads_sorted: false,
        dtype,
        chunks,
        dict: None,
    })
}

/// Two-pass Dictionary-layout chunk stream.
///
/// Pass 1 buffers quads in memory while incrementally collecting the unique
/// terms, spilling to a temp file (in arrival order) only when the dataset
/// outgrows one chunk — a dataset that fits never round-trips through rkyv
/// and the filesystem at all, the same lazy-spill rationale as `spill::Run`,
/// where the old unconditional spill paid a full serialize + write + read +
/// deserialize of every quad. The dictionary is then sorted and frozen.
/// Pass 2 lazily emits u32-encoded chunks from wherever pass 1 left the
/// quads. Peak memory: O(unique terms + chunk).
///
/// On `wasm32-unknown-unknown` (no filesystem, `spill` compiled out) the
/// in-memory pass covers datasets up to one chunk; the overflow that would
/// spill errors instead.
async fn build_dict_chunk_stream(
    mut quads: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<BuiltStream> {
    // ── Pass 1: buffer (spill only on overflow) + incremental dictionary ──
    let mut dict_builder = TermDictionaryBuilder::new();
    let mut buffer: Vec<RawQuad> = Vec::with_capacity(chunk_size.min(4096));
    let mut total = 0usize;
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    let mut spill: Option<DictSpill> = None;
    while let Some(res) = quads.next().await {
        let raw = res?;
        dict_builder.insert_quad(&raw);
        total += 1;
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        if let Some(spill) = spill.as_mut() {
            spill.writer.push(&raw)?;
            continue;
        }
        // Spill only to make room for a quad that would not fit, never merely
        // on reaching the chunk size — a dataset of exactly `chunk_size`
        // quads then stays in memory.
        if buffer.len() < chunk_size {
            buffer.push(raw);
            continue;
        }
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        return Err(crate::error::VortexRdfError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "streaming a Dictionary-layout dataset larger than one chunk spills to disk, \
             which is not available on wasm",
        )));
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            let mut sink = DictSpill::create(&buffer)?;
            sink.writer.push(&raw)?;
            spill = Some(sink);
            buffer = Vec::new();
        }
    }
    let (dict, id_map) = dict_builder.finish()?;
    let (dict, id_map) = (Arc::new(dict), Arc::new(id_map));
    log::debug!(
        "[UnsortedStreamBuilder] Ingested {} quads; dictionary of {} terms",
        total,
        dict.len()
    );

    // ── Pass 2: lazily emit encoded chunks from wherever pass 1 left off ──
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    let mut source = match spill {
        Some(sink) => sink.into_source()?,
        None => DictSource::Memory(buffer.into_iter()),
    };
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    let mut source = DictSource::Memory(buffer.into_iter());

    let buf = source.next_chunk(chunk_size)?;
    let first = if buf.is_empty() {
        dictionary::empty_struct(&indexes)?
    } else {
        // `total` is known from pass 1: a first chunk that covers everything
        // holds globally sorted index columns and gets them stamped.
        dictionary::build_chunk(
            &buf,
            &dict,
            &id_map,
            &indexes,
            0,
            false,
            total <= chunk_size,
        )?
    };
    let dtype = first.dtype().clone();
    let next_row = buf.len() as u32;
    drop(buf);

    let stream_dict = Arc::clone(&dict);
    let rest = stream::unfold(
        (source, stream_dict, id_map, indexes, next_row),
        move |(mut source, dict, id_map, indexes, row)| async move {
            let buf = match source.next_chunk(chunk_size) {
                Ok(b) => b,
                Err(e) => {
                    return Some((
                        Err(into_vortex_error(e)),
                        (source, dict, id_map, indexes, row),
                    ));
                }
            };
            if buf.is_empty() {
                return None;
            }
            let n = buf.len() as u32;
            let chunk = dictionary::build_chunk(&buf, &dict, &id_map, &indexes, row, false, false)
                .map_err(into_vortex_error);
            Some((chunk, (source, dict, id_map, indexes, row + n)))
        },
    );

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok(BuiltStream {
        components: Vec::new(),
        components_sorted: total <= chunk_size,
        quads_sorted: false,
        dtype,
        chunks,
        dict: Some(dict),
    })
}

/// Pass 1's overflow spill for [`build_dict_chunk_stream`]: the arrival-order
/// run every quad goes through once the dataset has outgrown one chunk.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
struct DictSpill {
    writer: RunWriter<RawQuad>,
    path: std::path::PathBuf,
    guard: TempRunsGuard,
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl DictSpill {
    /// Open the spill and move the already-buffered quads into it, so pass 2
    /// reads one uniform run.
    fn create(buffered: &[RawQuad]) -> Result<Self> {
        let temp_dir = make_temp_dir("unsorted_dict", None)?;
        let guard = TempRunsGuard {
            dir: temp_dir.clone(),
        };
        let path = temp_dir.join("quads.bin");
        let mut writer = RunWriter::create(&path)?;
        for quad in buffered {
            writer.push(quad)?;
        }
        Ok(Self {
            writer,
            path,
            guard,
        })
    }

    /// Close the run and hand it back as pass 2's read source.
    fn into_source(self) -> Result<DictSource> {
        self.writer.finish()?;
        Ok(DictSource::File {
            reader: RunReader::new(&self.path)?,
            _guard: self.guard,
        })
    }
}

/// Where pass 1 of [`build_dict_chunk_stream`] left the quads for pass 2's
/// encoding read-back: still in memory (the common, fits-in-one-chunk case)
/// or in the spill file, whose temp dir lives exactly as long as the reader.
enum DictSource {
    Memory(std::vec::IntoIter<RawQuad>),
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    File {
        reader: RunReader<RawQuad>,
        _guard: TempRunsGuard,
    },
}

impl DictSource {
    /// Pull up to `chunk_size` quads, in arrival order.
    fn next_chunk(&mut self, chunk_size: usize) -> Result<Vec<RawQuad>> {
        let mut buf = Vec::with_capacity(chunk_size.min(4096));
        while buf.len() < chunk_size {
            match self {
                DictSource::Memory(quads) => match quads.next() {
                    Some(q) => buf.push(q),
                    None => break,
                },
                #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
                DictSource::File { reader, .. } => match reader.next()? {
                    Some(q) => buf.push(q),
                    None => break,
                },
            }
        }
        Ok(buf)
    }
}

/// General chunk-stream path: quads are buffered as `RawQuad`s per chunk, as
/// required by index building (whole-chunk sorts) and the TypedObject layout.
async fn build_buffered_chunk_stream(
    mut quads: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<(DType, ChunkStream, bool)> {
    let mut buf: Vec<RawQuad> = Vec::with_capacity(chunk_size.min(4096));
    while buf.len() < chunk_size {
        match quads.next().await {
            Some(res) => buf.push(res?),
            None => break,
        }
    }
    // A full first chunk may still be the whole dataset (a stream of exactly
    // chunk_size quads); only a lookahead can tell. The peeked item, if any,
    // becomes the next chunk's first quad below.
    let pending = if buf.len() == chunk_size {
        quads.next().await
    } else {
        None
    };
    let whole_dataset = pending.is_none();

    let first = if buf.is_empty() {
        make_empty_struct(layout, &indexes)?
    } else {
        // Nothing follows the first chunk: it is the whole dataset and its
        // index columns are globally sorted (stamped for binary-search
        // routing).
        build_struct_array(&buf, layout, &indexes, 0, false, whole_dataset)?
    };
    let dtype = first.dtype().clone();
    let next_row = buf.len() as u32;
    drop(buf);

    let rest = stream::unfold(
        (quads, pending, layout, indexes, next_row),
        move |(mut quads, carried, layout, indexes, row)| async move {
            let mut buf: Vec<RawQuad> = Vec::with_capacity(chunk_size.min(4096));
            if let Some(res) = carried {
                match res {
                    Ok(q) => buf.push(q),
                    Err(e) => {
                        return Some((
                            Err(into_vortex_error(e)),
                            (quads, None, layout, indexes, row),
                        ));
                    }
                }
            }
            while buf.len() < chunk_size {
                match quads.next().await {
                    Some(Ok(q)) => buf.push(q),
                    Some(Err(e)) => {
                        return Some((
                            Err(into_vortex_error(e)),
                            (quads, None, layout, indexes, row),
                        ));
                    }
                    None => break,
                }
            }
            if buf.is_empty() {
                return None;
            }
            let n = buf.len();
            let chunk = build_struct_array(&buf, layout, &indexes, row, false, false)
                .map_err(into_vortex_error);
            Some((chunk, (quads, None, layout, indexes, row + n as u32)))
        },
    );

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok((dtype, chunks, whole_dataset))
}

/// Fast chunk-stream path for the Default layout without indexes: appends
/// term strings directly into per-column builders, skipping the `RawQuad`
/// intermediate (4 String allocations + frees per quad) entirely.
async fn build_direct_chunk_stream(
    mut quads: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    chunk_size: usize,
) -> Result<(DType, ChunkStream)> {
    let mut builder = DirectChunkBuilder::new(chunk_size.min(4096));
    while builder.len() < chunk_size {
        match quads.next().await {
            Some(res) => builder.push(&res?),
            None => break,
        }
    }

    let first = if builder.is_empty() {
        make_empty_struct(LayoutStrategy::Default, &Vec::new())?
    } else {
        builder.finish()?
    };
    let dtype = first.dtype().clone();

    let rest = stream::unfold(quads, move |mut quads| async move {
        let mut builder = DirectChunkBuilder::new(chunk_size.min(4096));
        while builder.len() < chunk_size {
            match quads.next().await {
                Some(Ok(q)) => builder.push(&q),
                Some(Err(e)) => return Some((Err(into_vortex_error(e)), quads)),
                None => break,
            }
        }
        if builder.is_empty() {
            return None;
        }
        Some((builder.finish().map_err(into_vortex_error), quads))
    });

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok((dtype, chunks))
}
