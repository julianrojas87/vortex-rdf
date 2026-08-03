//! The *residency* axis of the Dictionary layout: how a resolved store
//! reaches its term dictionary — held whole in memory, or left in the file's
//! dictionary child and probed on demand. The dictionary itself (storage,
//! FSST, probing, file-backing) lives in
//! [`term_dictionary`](crate::store::term_dictionary); this seam is what
//! couples it to the layout's pattern vocabulary.

use std::sync::Arc;

use crate::error::Result;
#[cfg(feature = "file-io")]
use crate::store::term_dictionary::FileBackedDict;
use crate::store::term_dictionary::TermDictionary;

/// How a resolved Dictionary layout reaches its term dictionary: the
/// *residency* axis, sitting above `TermStore`'s encoding axis.
///
/// `Resident` holds the whole dictionary in memory; `FileBacked` leaves the
/// terms in the file's scannable dictionary child and reads them on demand,
/// which makes term↔code translation asynchronous. The method contract that
/// keeps both arms behind one seam:
///
/// - [`resolve_pattern`](Self::resolve_pattern) is the **async prelude**: the
///   one place a dictionary is allowed to perform I/O during a match. It runs
///   before the synchronous match core and pre-resolves every bound term of
///   the pattern into the match's [`PatternCodes`](crate::store::layouts::PatternCodes), so everything downstream
///   resolves from that cache without touching the dictionary again — which
///   is what confines a file-backed dictionary's I/O to this method.
/// - The sync accessors ([`get_id`](Self::get_id), [`term_at`](Self::term_at))
///   are total for `Resident` and answer `None`/`Err` for `FileBacked` —
///   callers needing them on a file-backed store go through the async paths.
/// - [`resident`](Self::resident) hands out the in-memory dictionary itself
///   (`None` for `FileBacked`), for the paths that genuinely need the whole
///   column; [`ensure_resident`](Self::ensure_resident) lifts a file-backed
///   dictionary transiently when serialization must have it.
#[derive(Clone)]
pub(crate) enum DictAccess {
    /// The whole dictionary in memory (FSST-compressed or canonical).
    Resident(Arc<TermDictionary>),
    /// The dictionary left in its file, probed and decoded by scans on
    /// demand — chosen at open when the term count exceeds the residency
    /// threshold (see `VortexRdfStore::from_file`).
    #[cfg(feature = "file-io")]
    FileBacked(FileBackedDict),
}

impl DictAccess {
    /// Pre-resolve every bound term of `pattern` into `codes` — the async
    /// prelude run before the synchronous match core.
    ///
    /// For `Resident` the lookups are in-memory binary searches, resolved
    /// eagerly rather than lazily at each use site: what this buys is the
    /// invariant the match core is written against — *after the prelude,
    /// every bound role is in `codes`* — which is what lets a file-backed
    /// dictionary do its I/O here and nowhere else.
    pub(crate) async fn resolve_pattern(
        &self,
        pattern: super::QuadPattern<'_>,
        codes: &mut super::PatternCodes,
    ) -> Result<()> {
        use super::TermRef;
        match self {
            DictAccess::Resident(dict) => {
                if let Some(s) = pattern.subject {
                    codes.resolve(TermRef::Subject(s), |t| dict.get_id(t));
                }
                if let Some(p) = pattern.predicate {
                    codes.resolve(TermRef::Predicate(p), |t| dict.get_id(t));
                }
                if let Some(o) = pattern.object {
                    codes.resolve(TermRef::Object(o), |t| dict.get_id(t));
                }
                if let Some(g) = pattern.graph {
                    codes.resolve(TermRef::Graph(g), |t| dict.get_id(t));
                }
                Ok(())
            }
            // Each bound role costs one filtered probe of the term column
            // (memoized in the probe cache); the resolved code is then seeded
            // into `codes` so the sync match core never reaches back here.
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(fb) => {
                if let Some(s) = pattern.subject {
                    let id = fb.get_id(codes.render(TermRef::Subject(s))).await?;
                    codes.resolve(TermRef::Subject(s), |_| id);
                }
                if let Some(p) = pattern.predicate {
                    let id = fb.get_id(codes.render(TermRef::Predicate(p))).await?;
                    codes.resolve(TermRef::Predicate(p), |_| id);
                }
                if let Some(o) = pattern.object {
                    let id = fb.get_id(codes.render(TermRef::Object(o))).await?;
                    codes.resolve(TermRef::Object(o), |_| id);
                }
                if let Some(g) = pattern.graph {
                    let id = fb.get_id(codes.render(TermRef::Graph(g))).await?;
                    codes.resolve(TermRef::Graph(g), |_| id);
                }
                Ok(())
            }
        }
    }

    /// Look up a term's code through the *synchronous* surface
    /// (`VortexRdfStore::encode_code`). A file-backed dictionary cannot probe
    /// its file without I/O, so it answers `None` — callers needing
    /// file-backed lookups go through the async prelude instead.
    pub(crate) fn get_id(&self, term: &str) -> Option<u32> {
        match self {
            DictAccess::Resident(dict) => dict.get_id(term),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(_) => None,
        }
    }

    /// [`get_id`](Self::get_id) for the match core's probe closures, which run
    /// strictly after [`resolve_pattern`](Self::resolve_pattern) has seeded
    /// every bound role into the pattern's code cache — so a call ever
    /// reaching a file-backed dictionary is a broken prelude, not a miss.
    pub(crate) fn get_id_resolved(&self, term: &str) -> Option<u32> {
        match self {
            DictAccess::Resident(dict) => dict.get_id(term),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(_) => unreachable!(
                "the async prelude resolves every bound role before the sync match core runs"
            ),
        }
    }

    /// Decode a code back to its term string through the *synchronous*
    /// surface (`VortexRdfStore::decode_code`), or `None` when out of range —
    /// or when the dictionary is file-backed (same contract as
    /// [`get_id`](Self::get_id)).
    pub(crate) fn term_at(&self, code: u32) -> Option<String> {
        match self {
            DictAccess::Resident(dict) => dict.term_at(code),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(_) => None,
        }
    }

    /// The in-memory dictionary, or `None` when it is file-backed — sync
    /// callers (snapshots, in-memory chunk decode) treat `None` as "not
    /// available here"; paths that genuinely need the whole column go through
    /// [`ensure_resident`](Self::ensure_resident).
    pub(crate) fn resident(&self) -> Option<&Arc<TermDictionary>> {
        match self {
            DictAccess::Resident(dict) => Some(dict),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(_) => None,
        }
    }

    /// The whole dictionary in memory, lifting a file-backed one with a single
    /// term-column scan — for the operations that need the full column
    /// (serialization, compaction, tail-merge re-encoding). The lift is
    /// transient: it is not cached back into the access, so a store's steady
    /// state keeps the file-backed footprint.
    pub(crate) async fn ensure_resident(&self) -> Result<Arc<TermDictionary>> {
        match self {
            DictAccess::Resident(dict) => Ok(Arc::clone(dict)),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(fb) => Ok(Arc::new(fb.load_resident().await?)),
        }
    }

    /// Whether reconstruction must decode through the file (async) rather
    /// than the resident dictionary.
    #[cfg(feature = "file-io")]
    pub(crate) fn is_file_backed(&self) -> bool {
        matches!(self, DictAccess::FileBacked(_))
    }
}
